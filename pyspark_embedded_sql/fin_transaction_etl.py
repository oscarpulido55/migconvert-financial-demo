import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session for BigQuery. Hive-specific configurations are commented out as they are not applicable."""
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        # .enableHiveSupport() - BigQuery does not use Hive Metastore; Spark would use a BigQuery connector.
        # .config("hive.exec.dynamic.partition", "true") - Hive-specific partitioning configuration, not applicable for BigQuery.
        # .config("hive.exec.dynamic.partition.mode", "nonstrict") - Hive-specific partitioning configuration, not applicable for BigQuery.
        # .config("hive.exec.max.dynamic.partitions", "2000") - Hive-specific partitioning configuration, not applicable for BigQuery.
        # .config("hive.exec.max.dynamic.partitions.pernode", "256") - Hive-specific partitioning configuration, not applicable for BigQuery.
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # SET hiveconf:process_date='{process_date}' - Hive-specific variable setting; for BigQuery, process_date is directly embedded.

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
        FROM `fin_landing.raw_transactions` -- BigQuery uses backticks for table names, and 'dataset.table' for fully qualified names.
        WHERE CAST(transaction_timestamp AS DATE) = '{process_date}' -- Converted to_date() to CAST AS DATE and substituted Hive variable directly.
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # For BigQuery, INSERT OVERWRITE TABLE ... PARTITION is functionally equivalent to
    # a DELETE statement for existing partition data followed by an INSERT INTO for the new data.
    # This is presented as a multi-statement SQL string within the same variable for line-by-line conversion.
    insert_sql = f"""
        DELETE FROM `fin_core.fact_transactions` WHERE trx_date = '{process_date}';

        INSERT INTO `fin_core.fact_transactions` (
            trx_uuid, source_account_id, destination_account_id, transaction_type, amount_base_currency,
            normalized_usd_amount, currency_code, transaction_timestamp, merchant_category_code, channel,
            customer_id, customer_segment, kyc_status, rolling_10_trx_amount, status, trx_date, region_id
        )
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC) AS normalized_usd_amount, -- DECIMAL changed to NUMERIC in BigQuery.
            r.currency_code,
            r.transaction_timestamp,
            r.merchant_category_code,
            r.channel,
            c.customer_id,
            c.customer_segment,
            c.kyc_status,
            -- Rolling sum to flag consecutive large transactions (syntax compatible with BigQuery)
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,

            -- Partitioning Columns
            CAST(r.transaction_timestamp AS DATE) AS trx_date, -- Converted to_date() to CAST AS DATE.
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `fin_core.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id -- BigQuery uses `dataset.table` naming convention.
        LEFT JOIN `fin_core.dim_customers` c ON dim_a.customer_id = c.customer_id -- BigQuery uses `dataset.table` naming convention.
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
          AND CAST(r.transaction_timestamp AS DATE) = '{process_date}' -- Filter ensures only relevant partition data is processed for insert.
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Similar to fact_transactions, this uses DELETE then INSERT for the specific summary_date partition.
    risk_sql = f"""
        DELETE FROM `fin_mart.risk_daily_summary` WHERE summary_date = '{process_date}';

        INSERT INTO `fin_mart.risk_daily_summary` (
            customer_id, customer_segment, region_id, total_daily_transactions, total_daily_volume_usd,
            max_single_transaction_usd, high_risk_mcc_count, unique_destinations_count, daily_risk_flag, summary_date
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
            -- Flag for Review (syntax compatible with BigQuery)
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            '{process_date}' AS summary_date -- Substituted Hive variable directly.
        FROM `fin_core.fact_transactions` -- BigQuery uses `dataset.table` naming convention.
        WHERE trx_date = '{process_date}' -- Substituted Hive variable directly.
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

    # Initialize Datasets (Schema) for safety. In BigQuery, 'databases' are referred to as 'datasets'.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_landing;") # Converted CREATE DATABASE to CREATE SCHEMA.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_core;")    # Converted CREATE DATABASE to CREATE SCHEMA.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_mart;")     # Converted CREATE DATABASE to CREATE SCHEMA.

    run_etl_pipeline(sp, p_date)
    sp.stop()
