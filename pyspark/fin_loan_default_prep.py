from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, when, sum as _sum, count as _count, round, array, explode, create_map, lit
from pyspark.sql.types import IntegerType

def init_spark():
    return SparkSession.builder \
        .appName("Financial_Loan_Default_Feature_Prep") \
        .enableHiveSupport() \
        .getOrCreate()

def prepare_loan_features(spark):
    """
    Reads from multiple Hive tables, performs extensive data aggregation,
    pivot-like behavior, and creates a wide feature matrix for machine learning.
    """
    df_loans = spark.read.table("fin_core.dim_loans")
    df_customers = spark.read.table("fin_core.dim_customers")
    df_payments = spark.read.table("fin_core.fact_loan_payments")
    df_credit_bureau = spark.read.table("fin_core.external_credit_bureau")

    # 1. Customer Level Features
    cust_feat = df_customers.select(
        col("customer_id"),
        datediff(current_date(), col("date_of_birth")).alias("age_days"),
        col("customer_segment"),
        col("total_initial_deposit")
    ).withColumn("age_years", round(col("age_days") / 365, 0).cast(IntegerType()))

    # 2. Loan Payment History Aggregation
    pay_feat = df_payments.groupBy("loan_id").agg(
        _count("payment_id").alias("total_payments_made"),
        _sum("principal_amount").alias("total_principal_paid"),
        _sum("interest_amount").alias("total_interest_paid"),
        _sum(when(col("days_late") > 30, 1).otherwise(0)).alias("payments_30_days_late"),
        _sum(when(col("days_late") > 60, 1).otherwise(0)).alias("payments_60_days_late"),
        _sum(when(col("days_late") > 90, 1).otherwise(0)).alias("payments_90_days_late")
    )

    # 3. Join everything heavily onto Loans
    base_matrix = df_loans.alias("l") \
        .join(cust_feat.alias("c"), col("l.customer_id") == col("c.customer_id"), "left") \
        .join(pay_feat.alias("p"), col("l.loan_id") == col("p.loan_id"), "left") \
        .join(df_credit_bureau.alias("cb"), col("l.customer_id") == col("cb.customer_id"), "left")

    # 4. Feature Engineering: Debt to Income, LTV, and categorical encoding using PySpark when/otherwise
    final_features = base_matrix.select(
        col("l.loan_id"),
        col("l.customer_id"),
        col("l.loan_amount"),
        col("l.interest_rate"),
        col("cb.fico_score").alias("bureau_fico_score"),
        col("cb.number_of_open_lines"),
        col("cb.total_debt_balance"),
        col("c.age_years"),
        col("p.payments_30_days_late"),
        col("p.payments_90_days_late"),
        
        # Derived Metrics
        (col("l.loan_amount") / col("l.property_value")).alias("loan_to_value_ratio"),
        (col("cb.total_debt_balance") / col("cb.annual_income")).alias("debt_to_income_ratio"),
        
        # Target Variable
        when(col("l.loan_status") == 'DEFAULTED', 1).otherwise(0).alias("label_default")
    ).fillna({
        "payments_30_days_late": 0,
        "payments_90_days_late": 0,
        "bureau_fico_score": 600 # default interpolation
    })
    
    # 5. Save Wide Table back to Hive
    final_features.write \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("fin_mart.ml_loan_default_features")
        
    print(f"Feature engineering written out: {final_features.count()} rows")

if __name__ == "__main__":
    spark = init_spark()
    prepare_loan_features(spark)
    spark.stop()
