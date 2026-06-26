import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connection."""
    # Note: Replace 'your-gcp-project-id' with your actual GCP Project ID.
    # Note: Replace 'your-gcs-temp-bucket' with a GCS bucket for temporary BigQuery data.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.sql.extensions", "com.google.cloud.spark.bigquery.BigQuerySparkSessionExtension") \
        .config("spark.cloud.google.project.id", "your-gcp-project-id") \
        .config("spark.hadoop.google.cloud.project.id", "your-gcp-project-id") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.datasource.bigquery.temporaryGcsBucket", "your-gcs-temp-bucket") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using embedded BigQuery-compatible SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # Hive specific parameter setting removed. The process_date will be directly interpolated into SQL queries.
    # spark.sql(f"SET hiveconf:process_date='{process_date}'")

    # 1. Create temporary view for the delta transactions from daily landing zone
    logger.info("Creating temporary view for raw transactions...")
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW raw_trx_delta AS
        SELECT
            trx_uuid,
            source_account_id,
            destination_account_id,
            transaction_type,
            amount_base_currency,
            currency_code,
            exchange_rate,
            transaction_timestamp,
            merchant_category_code,
            channel,
            status,
            error_code
        FROM `your-gcp-project-id.fin_landing.raw_transactions`
        WHERE DATE(transaction_timestamp) = '{process_date}'
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    insert_sql = f"""
        -- Hive's PARTITION clause for dynamic partition overwrite is not directly supported by BigQuery SQL.
        -- If 'fin_core.fact_transactions' is partitioned by 'trx_date', Spark BigQuery connector might
        -- implicitly handle writes. For explicit partition overwrite, BigQuery SQL requires DML `DELETE WHERE`
        -- followed by `INSERT INTO`, or a specific WHERE filter in `INSERT OVERWRITE` if available.
        INSERT OVERWRITE `your-gcp-project-id.fin_core.fact_transactions`
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC) AS normalized_usd_amount,
            r.currency_code,
            r.transaction_timestamp,
            r.merchant_category_code,
            r.channel,
            c.customer_id,
            c.customer_segment,
            c.kyc_status,
            -- Rolling sum to flag consecutive large transactions
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,

            -- Partitioning Columns (ensure these match BigQuery table partitioning scheme)
            DATE(r.transaction_timestamp) AS trx_date,
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `your-gcp-project-id.fin_core.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN `your-gcp-project-id.fin_core.dim_customers` c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    risk_sql = f"""
        -- BigQuery allows `INSERT OVERWRITE ... WHERE` to target specific partitions.
        INSERT OVERWRITE `your-gcp-project-id.fin_mart.risk_daily_summary`
        WHERE summary_date = '{process_date}'
        SELECT
            customer_id,
            customer_segment,
            region_id,
            COUNT(trx_uuid) AS total_daily_transactions,
            SUM(normalized_usd_amount) AS total_daily_volume_usd,
            MAX(normalized_usd_amount) AS max_single_transaction_usd,
            COUNTIF(merchant_category_code IN ('7995', '6012')) AS high_risk_mcc_count,
            COUNT(DISTINCT destination_account_id) AS unique_destinations_count,
            -- Flag for Review
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            '{process_date}' AS summary_date
        FROM `your-gcp-project-id.fin_core.fact_transactions`
        WHERE trx_date = '{process_date}'
        GROUP BY
            customer_id,
            customer_segment,
            region_id
    """
    spark.sql(risk_sql)

    logger.info("Successfully completed ETL pipeline.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fin_transaction_etl.py <YYYY-MM-DD>")
        sys.exit(1)

    p_date = sys.argv[1]
    sp = create_spark_session()

    # Initialize DBs for safety - converting to CREATE SCHEMA for BigQuery datasets.
    # These operations will create BigQuery datasets if they don't exist in the configured project.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_landing")
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_core")
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_mart")

    run_etl_pipeline(sp, p_date)
    sp.stop()
