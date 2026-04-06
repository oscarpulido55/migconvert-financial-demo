def create_spark_session():
    """Initializes and returns a Spark session for BigQuery integration."""
    return SparkSession.builder \
        .appName("Financial_Credit_Card_Fraud_Scoring") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .config("gcpProjectId", "[YOUR_GCP_PROJECT_ID]") \
        .config("viewsEnabled", "true") \
        .getOrCreate()

full_cc_trx = spark.read.format("bigquery").option("table", "[YOUR_GCP_PROJECT_ID].fin_core.cc_transactions").load()
accounts = spark.read.format("bigquery").option("table", "[YOUR_GCP_PROJECT_ID].fin_core.dim_accounts").load()
merchants = spark.read.format("bigquery").option("table", "[YOUR_GCP_PROJECT_ID].fin_core.dim_merchants").load()

    final_output.write \
        .format("bigquery") \
        .option("table", "[YOUR_GCP_PROJECT_ID].fin_mart.fraud_scores_daily") \
        .mode("append") \
        .save()