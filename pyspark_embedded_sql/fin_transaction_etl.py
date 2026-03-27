import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session for BigQuery connection."""
    # To connect Spark with BigQuery, specify the BigQuery connector package.
    # Authentication usually relies on environment variables (e.g., GOOGLE_APPLICATION_CREDENTIALS)
    # or a service account key file. Ensure the Spark environment can access this file.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.1") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/your/service_account.json") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # For BigQuery, parameters are typically directly formatted into the SQL string
    # or passed as query parameters using the BigQuery client library,
    # rather than using Hive-specific 'SET hiveconf'.

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
        FROM fin_landing.raw_transactions
        WHERE DATE(transaction_timestamp) = '{process_date}' -- BigQuery uses DATE() to extract date from timestamp
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # Hive's INSERT OVERWRITE PARTITION is typically handled in BigQuery
    # by deleting specific partitions and then inserting new data.
    # Assumes fin_core.fact_transactions is partitioned by trx_date.
    spark.sql(f"DELETE FROM fin_core.fact_transactions WHERE trx_date = '{process_date}'")
    logger.info(f"Deleted existing data for trx_date = {process_date} from fin_core.fact_transactions.")

    insert_sql = f"""
        INSERT INTO fin_core.fact_transactions ( -- Explicit column list is good practice for BigQuery INSERT INTO
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
            -- Calculate normalized amount for aggregations (BigQuery uses BIGNUMERIC for high precision)
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC) AS normalized_usd_amount,
            r.currency_code,
            r.transaction_timestamp,
            r.merchant_category_code,
            r.channel,
            c.customer_id,
            c.customer_segment,
            c.kyc_status,
            -- Rolling sum to flag consecutive large transactions (syntax compatible)
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,

            -- Partitioning Columns: BigQuery DATE() function
            DATE(r.transaction_timestamp) AS trx_date,
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN fin_core.dim_customers c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Similar to fact_transactions, overwrite is handled by deleting specific partition then inserting.
    # Assumes fin_mart.risk_daily_summary is partitioned by summary_date.
    spark.sql(f"DELETE FROM fin_mart.risk_daily_summary WHERE summary_date = '{process_date}'")
    logger.info(f"Deleted existing data for summary_date = {process_date} from fin_mart.risk_daily_summary.")

    risk_sql = f"""
        INSERT INTO fin_mart.risk_daily_summary ( -- Explicit column list is good practice for BigQuery INSERT INTO
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
            -- Flag for Review (BigQuery compatible CASE statement)
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            '{process_date}' AS summary_date -- Directly pass date as string
        FROM fin_core.fact_transactions
        WHERE trx_date = '{process_date}' -- Directly pass date as string
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

    # BigQuery uses 'SCHEMA' instead of 'DATABASE'.
    # These commands create datasets if they don't exist in the configured Google Cloud Project.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_landing")
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_core")
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_mart")

    run_etl_pipeline(sp, p_date)
    sp.stop()