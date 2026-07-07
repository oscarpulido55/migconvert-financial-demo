    """Initializes and returns a Spark session with BigQuery connector support."""
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .config("spark.cloud.google.project.id", "your-gcp-project-id") \
    full_cc_trx = spark.read.format("bigquery").option("table", "your-gcp-project-id.fin_core.cc_transactions").load()
    accounts = spark.read.format("bigquery").option("table", "your-gcp-project-id.fin_core.dim_accounts").load()
    merchants = spark.read.format("bigquery").option("table", "your-gcp-project-id.fin_core.dim_merchants").load()
            .format("bigquery") \
            .option("table", "your-gcp-project-id.fin_mart.fraud_scores_daily") \
            .save()
