import pandas as pd
import numpy as np
from datetime import timedelta

import btyd
from btyd.fitters.beta_geo_fitter import BetaGeoFitter
from btyd import GammaGammaFitter

from btyd.plotting import plot_calibration_purchases_vs_holdout_purchases
from btyd.plotting import plot_probability_alive_matrix
from btyd.plotting import plot_frequency_recency_matrix

import matplotlib.pyplot as plt

import pyspark.sql.functions as fn
from pyspark.sql.types import *

import mlflow.pyfunc
import mlflow

xlsx_filename = "/dbfs/tmp/clv/online_retail/Online Retail.xlsx"

# schema of the excel spreadsheet data range
orders_schema = {
    'InvoiceNo': str,
    'StockCode': str,
    'Description': str,
    'Quantity': np.int64,
    'InvoiceDate': np.datetime64,
    'UnitPrice': np.float64,
    'CustomerID': str,
    'Country': str
}

# read spreadsheet to pandas dataframe
# the xlrd library must be installed for this step to work
orders_pd = pd.read_excel(
    xlsx_filename,
    sheet_name='Online Retail',
    header=0,  # first row is header
    dtype=orders_schema
)

# calculate sales amount as quantity * unit price
orders_pd['SalesAmount'] = orders_pd['Quantity'] * orders_pd['UnitPrice']

# convert pandas DF to Spark DF
orders = spark.createDataFrame(orders_pd)

# present Spark DF as queryable view
orders.createOrReplaceTempView('orders')

# identify outlier customers
customers_to_exclude = (
    orders
    .groupBy('customerid', 'invoicedate')
    .agg(fn.sum('salesamount').alias('salesamount'))
    .filter('salesamount=70000')
    .select('customerid')
    .distinct()
)

# remove bad records and outlier customers
cleansed_orders = (
    orders
    .filter('customerid is not null')
    .join(
        customers_to_exclude,
        on='customerid',
        how='leftanti'
    )
)

# reload orders pandas dataframe from cleansed data
orders_pd = cleansed_orders.toPandas()

# make cleansed data accessible for queries
_ = cleansed_orders.createOrReplaceTempView('orders')

# set the last transaction date as the end point for this historical dataset
current_date = orders_pd['InvoiceDate'].max()

# calculate the required customer metrics
metrics_pd = (
    btyd.utils.summary_data_from_transaction_data(
        orders_pd,
        customer_id_col='CustomerID',
        datetime_col='InvoiceDate',
        observation_period_end=current_date,
        freq='D',
        monetary_value_col='SalesAmount'  # use sales amount to determine monetary value
    )
)

# programmatic sql api calls to derive summary customer stats
# valid customer orders
x = (
    orders
    .withColumn('transaction_at', fn.to_date('invoicedate'))
    .groupBy('customerid', 'transaction_at')
    .agg(fn.sum('salesamount').alias('salesamount'))  # SALES AMOUNT
)

# calculate last date in dataset
y = (
    orders
    .groupBy()
    .agg(fn.max(fn.to_date('invoicedate')).alias('current_dt'))
)

# calculate first transaction date by customer
z = (
    orders
    .groupBy('customerid')
    .agg(fn.min(fn.to_date('invoicedate')).alias('first_at'))
)

# combine customer history with date info
a = (x
.crossJoin(y)
.join(z, on='customerid', how='inner')
.selectExpr(
    'customerid',
    'first_at',
    'transaction_at',
    'salesamount',
    'current_dt'
)
)

# calculate relevant metrics by customer
metrics_api = (a
               .groupBy(a.customerid, a.current_dt, a.first_at)
               .agg(
    (
            fn.countDistinct(a.transaction_at) - 1).cast(FloatType()).alias('frequency'),
    fn.datediff(fn.max(a.transaction_at), a.first_at).cast(FloatType()).alias('recency'),
    fn.datediff(a.current_dt, a.first_at).cast(FloatType()).alias('T'),
    fn.when(fn.countDistinct(a.transaction_at) == 1, 0)  # MONETARY VALUE
    .otherwise(
        fn.sum(
            fn.when(a.first_at == a.transaction_at, 0)
            .otherwise(a.salesamount)
        ) / (fn.countDistinct(a.transaction_at) - 1)
    ).alias('monetary_value')
)
               .select('customerid', 'frequency', 'recency', 'T', 'monetary_value')
               .orderBy('customerid')
               )

# set the last transaction date as the end point for this historical dataset
current_date = orders_pd['InvoiceDate'].max()

# define end of calibration period
calibration_end_date = current_date - timedelta(days=holdout_days)

# calculate the required customer metrics
metrics_cal_pd = (
    btyd.utils.calibration_and_holdout_data(
        orders_pd,
        customer_id_col='CustomerID',
        datetime_col='InvoiceDate',
        observation_period_end=current_date,
        calibration_period_end=calibration_end_date,
        freq='D',
        monetary_value_col='SalesAmount'  # use sales amount to determine monetary value
    )
)

# display first few rows
metrics_cal_pd.head(10)

# valid customer orders
x = (
    orders
    .withColumn('transaction_at', fn.to_date('invoicedate'))
    .groupBy('customerid', 'transaction_at')
    .agg(fn.sum('salesamount').alias('salesamount'))
)

# calculate last date in dataset
y = (
    orders
    .groupBy()
    .agg(fn.max(fn.to_date('invoicedate')).alias('current_dt'))
)

# calculate first transaction date by customer
z = (
    orders
    .groupBy('customerid')
    .agg(fn.min(fn.to_date('invoicedate')).alias('first_at'))
)

# combine customer history with date info (CUSTOMER HISTORY)
p = (x
     .crossJoin(y)
     .join(z, on='customerid', how='inner')
     .withColumn('duration_holdout', fn.lit(holdout_days))
     .select(
    'customerid',
    'first_at',
    'transaction_at',
    'current_dt',
    'salesamount',
    'duration_holdout'
)
     .distinct()
     )

# calculate relevant metrics by customer
# note: date_sub requires a single integer value unless employed within an expr() call
a = (p
.where(p.transaction_at < fn.expr('date_sub(current_dt, duration_holdout)'))
.groupBy(p.customerid, p.current_dt, p.duration_holdout, p.first_at)
.agg(
    (fn.countDistinct(p.transaction_at) - 1).cast(FloatType()).alias('frequency_cal'),
    fn.datediff(fn.max(p.transaction_at), p.first_at).cast(FloatType()).alias('recency_cal'),
    fn.datediff(fn.expr('date_sub(current_dt, duration_holdout)'), p.first_at).cast(FloatType()).alias('T_cal'),
    fn.when(fn.countDistinct(p.transaction_at) == 1, 0)
    .otherwise(
        fn.sum(
            fn.when(p.first_at == p.transaction_at, 0)
            .otherwise(p.salesamount)
        ) / (fn.countDistinct(p.transaction_at) - 1)
    ).alias('monetary_value_cal')
)
)

b = (p
.where((p.transaction_at >= fn.expr('date_sub(current_dt, duration_holdout)')) & (p.transaction_at <= p.current_dt))
.groupBy(p.customerid)
.agg(
    fn.countDistinct(p.transaction_at).cast(FloatType()).alias('frequency_holdout'),
    fn.avg(p.salesamount).alias('monetary_value_holdout')
)
)

metrics_cal_api = (
    a
    .join(b, on='customerid', how='left')
    .select(
        'customerid',
        'frequency_cal',
        'recency_cal',
        'T_cal',
        'monetary_value_cal',
        fn.coalesce(b.frequency_holdout, fn.lit(0.0)).alias('frequency_holdout'),
        fn.coalesce(b.monetary_value_holdout, fn.lit(0.0)).alias('monetary_value_holdout'),
        'duration_holdout'
    )
    .orderBy('customerid')
)

# remove customers with no repeats (complete dataset)
filtered_pd = metrics_pd[metrics_pd['frequency'] > 0]
filtered = metrics_api.where(metrics_api.frequency > 0)

## remove customers with no repeats in calibration period
filtered_cal_pd = metrics_cal_pd[metrics_cal_pd['frequency_cal'] > 0]
filtered_cal = metrics_cal_api.where(metrics_cal_api.frequency_cal > 0)

# exclude dates with negative totals (see note above)
filtered = filtered.where(filtered.monetary_value > 0)
filtered_cal = filtered_cal.where(filtered_cal.monetary_value_cal > 0)


