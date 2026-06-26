from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, year, month, dayofmonth, max as spark_max, sum as spark_sum, broadcast, when, coalesce, udf, array_contains, explode
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField, ArrayType

def get_spark_session():
    """Initializes and returns a Spark session with BigQuery support enabled.""" # Updated comment for BigQuery context
    return SparkSession.builder \
        .appName("Financial_Customer_Onboarding_ETL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0") \
        .config("spark.cloud.google.project.id", "your-gcp-project-id") \
        .config("spark.hadoop.google.cloud.project.id", "your-gcp-project-id") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_customer_onboarding(spark):
    """
    Main ETL process for customer onboarding.
    Reads raw customer data, KYC records, and initial funding details,
    performs complex transformations, and writes to the dimensions table.
    """
    # 1. Read Raw Data sources (Reads from BigQuery tables)
    raw_customers_df = spark.read.format("bigquery").option("table", "your-gcp-project-id.landing_zone.raw_customers").load() # Reads from BigQuery table instead of HDFS JSON files
    raw_kyc_df = spark.read.format("bigquery").option("table", "your-gcp-project-id.landing_zone.raw_kyc_status").load() # Reads from BigQuery table instead of HDFS Parquet files
    raw_accounts_df = spark.read.format("bigquery").option("table", "your-gcp-project-id.landing_zone.raw_accounts").load() # Reads from BigQuery table instead of HDFS CSV files

    # 2. Extract latest KYC status using Window Functions
    kyc_window = Window.partitionBy("customer_id").orderBy(col("verification_date").desc())
    latest_kyc_df = raw_kyc_df \
        .withColumn("row_num", spark_max("verification_date").over(kyc_window)) \
        .filter(col("verification_date") == col("row_num")) \
        .drop("row_num") \
        .withColumnRenamed("status", "kyc_status") \
        .withColumnRenamed("risk_rating", "kyc_risk_rating")

    # 3. Aggregate Initial Funding
    funding_agg_df = raw_accounts_df \
        .filter(col("account_status") == "ACTIVE") \
        .groupBy("customer_id") \
        .agg(
            spark_sum("initial_deposit").alias("total_initial_deposit"),
            spark_max("open_date").alias("last_account_open_date")
        )

    # 4. Join and Enrich
    # Broadcast join for smaller KYC dimension against larger Customers table
    enriched_customer_df = raw_customers_df.alias("c") \
        .join(broadcast(latest_kyc_df).alias("k"), col("c.customer_id") == col("k.customer_id"), "left_outer") \
        .join(funding_agg_df.alias("f"), col("c.customer_id") == col("f.customer_id"), "left_outer")

    # 5. Complex Transformations: Calculate customer segments and risk profiles
    final_dim_customers = enriched_customer_df.select(
        col("c.customer_id"),
        col("c.first_name"),
        col("c.last_name"),
        col("c.ssn_hash").alias("national_id_hash"),
        col("c.dob").alias("date_of_birth"),
        col("c.address.country").alias("country_code"),
        col("c.address.state").alias("state_code"),
        coalesce(col("k.kyc_status"), lit("PENDING")).alias("kyc_status"),
        coalesce(col("k.kyc_risk_rating"), lit("UNKNOWN")).alias("risk_rating"),
        coalesce(col("f.total_initial_deposit"), lit(0.0)).alias("total_initial_deposit"),
        col("f.last_account_open_date"),
        current_timestamp().alias("etl_insert_ts")
    ).withColumn(
        "customer_segment",
        when(col("total_initial_deposit") > 1000000, "PRIVATE_WEALTH")
        .when((col("total_initial_deposit") >= 100000) & (col("total_initial_deposit") <= 1000000), "PREMIUM")
        .when(col("kyc_status") == "REJECTED", "RESTRICTED")
        .otherwise("RETAIL")
    ).withColumn(
        "onboarding_year", year(col("last_account_open_date"))
    ).withColumn(
        "onboarding_month", month(col("last_account_open_date"))
    ).withColumn( # Added column for BigQuery date partitioning
        "onboarding_date", to_date(col("last_account_open_date"))
    )

    # 6. Write to Managed BigQuery Table
    # The target is your-gcp-project-id.fin_core.dim_customers partitioned by onboarding_date and clustered by country_code, onboarding_year, onboarding_month
    final_dim_customers.write \
        .format("bigquery") \
        .option("table", "your-gcp-project-id.fin_core.dim_customers") \
        .option("temporaryGcsBucket", "your-gcs-bucket-for-staging") \
        .option("writeDisposition", "OVERWRITE") \
        .option("partitionField", "onboarding_date") \
        .option("clusteredFields", "country_code,onboarding_year,onboarding_month") \
        .mode("overwrite") \
        .save() # Use save() for BigQuery connector writes

    print(f"Successfully processed {final_dim_customers.count()} customer records.")

if __name__ == "__main__":
    spark = get_spark_session()

    # Optional: BigQuery datasets must exist; this Hive command is not applicable for BigQuery.
    # spark.sql("CREATE DATABASE IF NOT EXISTS fin_core") # Replaced Hive 'CREATE DATABASE' with a commented line; BigQuery schemas (datasets) are typically managed externally.

    process_customer_onboarding(spark)
    spark.stop()
