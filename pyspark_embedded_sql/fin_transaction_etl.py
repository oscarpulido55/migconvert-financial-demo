import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Placeholder for your Google Cloud Project ID. This must be set correctly.
# It can also be picked up from the environment (e.g., GOOGLE_CLOUD_PROJECT).
# REQUIRED: Replace with your actual GCP Project ID.
GCP_PROJECT_ID = "your_gcp_project_id"

def create_spark_session():
    """Initializes Spark Session with BigQuery connector support."""
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.cloud.google.project.id", GCP_PROJECT_ID) \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery-compatible SQL queries via Spark.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # Hive-specific 'SET hiveconf' removed. Process date is passed directly using f-strings.

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
        FROM `{GCP_PROJECT_ID}`.fin_landing.raw_transactions
        WHERE DATE(transaction_timestamp) = DATE('{process_date}')
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into `{GCP_PROJECT_ID}`.fin_core.fact_transactions...")
    # BigQuery DML for dynamic partition overwrite, typically seen as `INSERT OVERWRITE TABLE ... PARTITION` in Hive,
    # often involves an explicit DELETE of target partitions followed by an INSERT in BigQuery.
    # Assumes `fact_transactions` table in BigQuery is partitioned by `trx_date`.

    # Delete existing data for the process_date
    spark.sql(f"DELETE FROM `{GCP_PROJECT_ID}`.fin_core.fact_transactions WHERE trx_date = DATE('{process_date}')")

    insert_sql = f"""
        INSERT INTO `{GCP_PROJECT_ID}`.fin_core.fact_transactions
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations (BigQuery uses NUMERIC for fixed-point decimals)
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC) AS normalized_usd_amount,
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

            -- Partitioning Columns
            DATE(r.transaction_timestamp) AS trx_date,
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `{GCP_PROJECT_ID}`.fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN `{GCP_PROJECT_ID}`.fin_core.dim_customers c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Similar to above, replacing Hive's `INSERT OVERWRITE TABLE ... PARTITION` with DELETE + INSERT.
    # Assumes `risk_daily_summary` table in BigQuery is partitioned by `summary_date`.

    # Delete existing data for the process_date
    spark.sql(f"DELETE FROM `{GCP_PROJECT_ID}`.fin_mart.risk_daily_summary WHERE summary_date = DATE('{process_date}')")

    risk_sql = f"""
        INSERT INTO `{GCP_PROJECT_ID}`.fin_mart.risk_daily_summary
        SELECT
            customer_id,
            customer_segment,
            region_id,
            COUNT(trx_uuid) AS total_daily_transactions,
            SUM(normalized_usd_amount) AS total_daily_volume_usd,
            MAX(normalized_usd_amount) AS max_single_transaction_usd,
            COUNT(CASE WHEN merchant_category_code IN ('7995', '6012') THEN 1 END) AS high_risk_mcc_count,
            COUNT(DISTINCT destination_account_id) AS unique_destinations_count,
            -- Flag for Review
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            DATE('{process_date}') AS summary_date -- Using f-string for date value directly in BigQuery DML
        FROM `{GCP_PROJECT_ID}`.fin_core.fact_transactions
        WHERE trx_date = DATE('{process_date}')
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

    # Initialize DBs for safety
    # In BigQuery, databases are referred to as datasets. `CREATE SCHEMA` command in Spark SQL
    # with the BigQuery connector creates a BigQuery dataset.
    sp.sql(f"CREATE SCHEMA IF NOT EXISTS `{GCP_PROJECT_ID}`.fin_landing")
    sp.sql(f"CREATE SCHEMA IF NOT EXISTS `{GCP_PROJECT_ID}`.fin_core")
    sp.sql(f"CREATE SCHEMA IF NOT EXISTS `{GCP_PROJECT_ID}`.fin_mart")

    run_etl_pipeline(sp, p_date)
    sp.stop()
