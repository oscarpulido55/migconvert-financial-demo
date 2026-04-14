import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, unix_timestamp, count, avg, stddev_samp, when, date_sub
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

def create_spark_session():
    """Initializes and returns a Spark session with BigQuery support.""" # Changed comment to reflect BigQuery support
    return SparkSession.builder \
        .appName("Financial_Credit_Card_Fraud_Scoring") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.1") \
        .getOrCreate() # Replaced .enableHiveSupport() with BigQuery connector package configuration. Note: The specific version of the BigQuery connector (e.g., 0.29.1) may need to be updated based on your Spark and Scala versions. Ensure proper authentication (e.g., via Google Cloud credentials or service account JSON keyfile specified in Spark configs) is set up for Spark to access BigQuery.

# Create a custom UDF for great circle distance
def haversine(lat1, lon1, lat2, lon2):
    """Calculates the great circle distance between two points on the earth."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return -1.0
    R = 6371.0 # Radius of earth in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

haversine_udf = udf(haversine, DoubleType())

def score_transactions_for_fraud(spark, execution_date):
    """
    Uses pure PySpark DataFrame APIs to score credit card transactions.
    Replaces embedded SQL with continuous DataFrame transformations.
    """
    
    # 1. Load Data
    full_cc_trx = spark.table("fin_core.cc_transactions") # spark.table() can be used for BigQuery tables if the SparkSession is configured with the BigQuery connector and appropriate project/dataset settings.
    accounts = spark.table("fin_core.dim_accounts") # spark.table() can be used for BigQuery tables if the SparkSession is configured.
    merchants = spark.table("fin_core.dim_merchants") # spark.table() can be used for BigQuery tables if the SparkSession is configured.
    
    # 2. Extract current day transactions
    cc_trx = full_cc_trx.filter(col("trx_date") == execution_date)
    
    # 3. Join location data and calculate distance Native
    enriched_trx = cc_trx.alias("t") \
        .join(accounts.alias("a"), col("t.account_id") == col("a.account_id"), "inner") \
        .join(merchants.alias("m"), col("t.merchant_id") == col("m.merchant_id"), "left") \
        .withColumn(
            "distance_from_home_km",
            haversine_udf(col("a.home_lat"), col("a.home_lon"), col("m.merchant_lat"), col("m.merchant_lon"))
        )
        
    # 4. Pure DataFrame Historical Profiling Window
    # Filter for the last 90 days of transactions (excluding execution date)
    hist_trx = full_cc_trx.filter(
        (col("trx_date") >= date_sub(lit(execution_date), 90)) & 
        (col("trx_date") <= date_sub(lit(execution_date), 1))
    )
    
    # Aggregate to build the historical profile
    hist_profile = hist_trx.groupBy("account_id").agg(
        avg("amount").alias("avg_trx_amount_90d"),
        stddev_samp("amount").alias("stddev_trx_amount_90d"),
        (count("trx_id") / 90.0).alias("avg_daily_trx_count")
    ).fillna(0.0, subset=["stddev_trx_amount_90d"])
    
    # 5. Join current day transactions with their historical profiles
    df_features = enriched_trx.alias("curr") \
        .join(hist_profile.alias("hist"), col("curr.account_id") == col("hist.account_id"), "left")
        
    # 6. Apply Time-based Window Function (Last Hour Trx Count)
    time_window = Window.partitionBy("curr.account_id").orderBy(unix_timestamp("curr.trx_timestamp")).rangeBetween(-3600, 0)
    
    # 7. Apply Complex Business Logic and Scoring Native DataFrame API
    scored_df = df_features \
        .withColumn("trx_last_hour_cnt", count("curr.trx_id").over(time_window)) \
        .withColumn("amount_z_score", 
            when(col("hist.stddev_trx_amount_90d") > 0, 
                 (col("curr.amount") - col("hist.avg_trx_amount_90d")) / col("hist.stddev_trx_amount_90d"))
            .otherwise(lit(0.0))
        ) \
        .withColumn("distance_risk_score", 
            when((col("distance_from_home_km") > 500) & (col("distance_from_home_km") != -1.0), 30).otherwise(0)
        ) \
        .withColumn("amount_risk_score",
            when(col("amount_z_score") > 3.0, 40)
            .when(col("amount_z_score") > 2.0, 20)
            .otherwise(0)
        ) \
        .withColumn("velocity_risk_score",
            when(col("trx_last_hour_cnt") > 5, 30)
            .when(col("trx_last_hour_cnt") > 3, 15)
            .otherwise(0)
        ) \
        .withColumn("fraud_score", 
            col("distance_risk_score") + col("amount_risk_score") + col("velocity_risk_score")
        ) \
        .withColumn("is_fraud_alert", col("fraud_score") >= 60)
        
    # 8. Select final columns and write to target
    final_output = scored_df.select(
        "curr.trx_id", "curr.account_id", "curr.amount", 
        "distance_from_home_km", "amount_z_score", "trx_last_hour_cnt", 
        "fraud_score", "is_fraud_alert", lit(execution_date).alias("scoring_date")
    )
    
    final_output.write \
        .format("bigquery") \
        .option("table", "fin_mart.fraud_scores_daily") \
        .mode("append") \
        .save() # Changed from .insertInto() to .format("bigquery").option("table", ...).save() for writing to BigQuery. The "fin_mart.fraud_scores_daily" string should refer to your BigQuery dataset.table identifier (e.g., "your-gcp-project.fin_mart.fraud_scores_daily" if a default project is not configured).
    
    print(f"Pure DataFrame Fraud scoring completed for {execution_date}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pyspark fin_credit_card_fraud_scoring.py <YYYY-MM-DD>")
        sys.exit(1)
        
    exec_date = sys.argv[1]
    sp = create_spark_session()
    
    score_transactions_for_fraud(sp, exec_date)
    sp.stop()