from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, year, month, dayofmonth, max as spark_max, sum as spark_sum, broadcast, when, coalesce, udf, array_contains, explode
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField, ArrayType

def get_spark_session():
    """Initializes and returns a Spark session with BigQuery connector configured."""
    # Major Change: Removed enableHiveSupport() and added BigQuery connector configurations.
    # Required Config: Replace 'your-gcp-project-id' with your actual Google Cloud Project ID.
    # This configuration pulls the necessary BigQuery connector JARs.
    return SparkSession.builder \
        .appName("Financial_Customer_Onboarding_ETL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("parentProject", "your-gcp-project-id") \
        .getOrCreate()

def process_customer_onboarding(spark):
    """
    Main ETL process for customer onboarding.
    Reads raw customer data, KYC records, and initial funding details from GCS,
    performs complex transformations, and writes to a BigQuery table.
    """
    # 1. Read Raw Data sources (Simulated paths in GCS)
    # Major Change: Changed HDFS paths to Google Cloud Storage (GCS) paths.
    # Required Config: Replace 'your-gcs-bucket' with an existing GCS bucket where data is located.
    raw_customers_df = spark.read.json("gs://your-gcs-bucket/landing/fin/customers/")
    raw_kyc_df = spark.read.parquet("gs://your-gcs-bucket/landing/fin/kyc_status/")
    raw_accounts_df = spark.read.csv("gs://your-gcs-bucket/landing/fin/accounts/", header=True, inferSchema=True)

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
    )

    # 6. Write to BigQuery Table
    # The target is fin_core.dim_customers, with BigQuery handling partitioning based on its table definition.
    # Major Change: Changed output format from ORC to BigQuery connector.
    # `partitionBy` is removed because BigQuery table partitioning is defined at the BigQuery table level,
    # not typically through the Spark write operation for direct BigQuery writes.
    # Required Config: Replace 'your-gcp-project-id' with your actual Google Cloud Project ID and
    # 'your-gcs-temp-bucket' with an existing GCS bucket for temporary data staging by the connector.
    final_dim_customers.write \
        .format("bigquery") \
        .option("table", "your-gcp-project-id:fin_core.dim_customers") \
        .option("temporaryGcsBucket", "your-gcs-temp-bucket") \
        .mode("overwrite") \
        .save()

    print(f"Successfully processed {final_dim_customers.count()} customer records.")

if __name__ == "__main__":
    spark = get_spark_session()
    
    # Optional: Setup BigQuery Dataset (equivalent to Hive Database)
    # Major Change: Replaced Hive's CREATE DATABASE with BigQuery's CREATE SCHEMA (dataset).
    spark.sql("CREATE SCHEMA IF NOT EXISTS fin_core")
    
    process_customer_onboarding(spark)
    spark.stop()