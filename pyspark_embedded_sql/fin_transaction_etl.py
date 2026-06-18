import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session for BigQuery connection."""
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        .getOrCreate()
    # BigQuery connection requires the BigQuery connector JARs (e.g., spark-bigquery-with-dependencies)
    # to be present in the Spark classpath, typically added via spark-submit --packages.
    # No explicit .enableHiveSupport() or Hive-specific configs are needed for BigQuery.
    # Optional: Configure default BigQuery project/temporary GCS bucket if not handled externally.
    # .config("spark.sql.catalog.bigquery", "com.google.cloud.spark.bigquery.BigQuerySparkSessionExtension") \
    # .config("spark.sql.catalog.bigquery.project", "your-gcp-project-id") \
    # .config("spark.sql.catalog.bigquery.temporaryGcsBucket", "your-gcs-bucket-for-temp-files") \

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # Parameters for BigQuery SQL are typically injected directly using f-strings or parameterized queries.
    # BigQuery does not use 'hiveconf' variables.
    # spark.sql(f"SET hiveconf:process_date='{process_date}'") # Removed Hive-specific SET command

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
        FROM `fin_landing.raw_transactions` -- BigQuery table names often use backticks for project.dataset.table or dataset.table.
        WHERE DATE(transaction_timestamp) = '{process_date}' -- BigQuery uses DATE() function for date extraction. Direct f-string injection replaces '${hiveconf:process_date}'.
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # For partitioned tables in BigQuery, 'INSERT OVERWRITE TABLE ... PARTITION' from HiveQL is typically handled
    # by a DELETE statement for the target partition(s) followed by an INSERT INTO statement.
    spark.sql(f"""
        DELETE FROM `fin_core.fact_transactions`
        WHERE trx_date = '{process_date}'
    """)
    insert_sql = f"""
        INSERT INTO `fin_core.fact_transactions` (
            trx_uuid,
            source_account_id,
            destination_account_id,
            transaction_type,
            amount_base_currency,
            normalized_usd_amount,
            currency_code,
            transaction_timestamp,
            merchant_category_code,
            channel,
            customer_id,
            customer_segment,
            kyc_status,
            rolling_10_trx_amount,
            status,
            trx_date,
            region_id
        )
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC(18, 4)) AS normalized_usd_amount, -- DECIMAL in Hive is typically NUMERIC in BigQuery.
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
            DATE(r.transaction_timestamp) AS trx_date, -- BigQuery DATE function.
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `fin_core.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id -- Backticks for BigQuery tables.
        LEFT JOIN `fin_core.dim_customers` c ON dim_a.customer_id = c.customer_id -- Backticks for BigQuery tables.
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
          AND DATE(r.transaction_timestamp) = '{process_date}' -- Explicitly filter for the process_date to match the DELETE and target partition.
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Similar to above, for daily partition overwrite, use DELETE then INSERT.
    spark.sql(f"""
        DELETE FROM `fin_mart.risk_daily_summary`
        WHERE summary_date = '{process_date}'
    """)
    risk_sql = f"""
        INSERT INTO `fin_mart.risk_daily_summary` (
            customer_id,
            customer_segment,
            region_id,
            total_daily_transactions,
            total_daily_volume_usd,
            max_single_transaction_usd,
            high_risk_mcc_count,
            unique_destinations_count,
            daily_risk_flag,
            summary_date
        )
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
            '{process_date}' AS summary_date -- Direct f-string injection.
        FROM `fin_core.fact_transactions` -- Backticks for BigQuery tables.
        WHERE trx_date = '{process_date}' -- Direct f-string injection for partition filtering.
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

    # BigQuery uses DATASETS (or SCHEMAS) instead of DATABASES.
    # The Spark BigQuery connector maps Spark schemas to BigQuery datasets.
    # CREATE SCHEMA IF NOT EXISTS maps directly to creating a BigQuery dataset if it doesn't exist.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_landing")
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_core")
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_mart")

    run_etl_pipeline(sp, p_date)
    sp.stop()
