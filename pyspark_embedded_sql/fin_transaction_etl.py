import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connection and necessary configurations."""
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL_BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("temporaryGcsBucket", "your-gcs-bucket-for-spark-temp-data") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # For BigQuery, parameters are typically injected directly into the SQL string or via Spark's DataFrame API.
    # The Hive `SET hiveconf:` is not directly applicable.

    # 1. Create temporary view for the delta transactions from daily landing zone
    logger.info("Creating temporary view for raw transactions...")
    # BigQuery uses DATE() or CAST(... AS DATE) for date extraction. SparkSQL will interpret CREATE TEMPORARY VIEW.
    spark.sql(f"""
        CREATE TEMPORARY VIEW raw_trx_delta AS
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
        FROM fin_landing.raw_transactions -- Assuming fin_landing is a BigQuery dataset
        WHERE DATE(transaction_timestamp) = '{process_date}'
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # Hive's INSERT OVERWRITE PARTITION does not have a direct single-statement equivalent in BigQuery
    # for existing partitioned tables if only specific partitions are being overwritten.
    # The closest behavior in BigQuery is often achieved by an explicit DELETE for the target partitions
    # followed by an INSERT INTO, or using CREATE OR REPLACE TABLE AS SELECT (for full table replacement).
    # This conversion uses INSERT INTO, implying appending.
    # For full Hive INSERT OVERWRITE PARTITION semantics, an explicit DELETE statement for `trx_date = '{process_date}'`
    # and `region_id` derived from `raw_trx_delta` should precede this INSERT.
    insert_sql = f"""
        INSERT INTO fin_core.fact_transactions (
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
            trx_date, -- BigQuery partition columns are part of the select list
            region_id
        )
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC(18, 4)) AS normalized_usd_amount,
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
            DATE(r.transaction_timestamp) AS trx_date, -- BigQuery uses DATE() for date part extraction
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
    # Similar to above, BigQuery INSERT INTO will append. For Hive's INSERT OVERWRITE PARTITION semantics,
    # an explicit DELETE for `summary_date = '{process_date}'` should precede this INSERT.
    risk_sql = f"""
        INSERT INTO fin_mart.risk_daily_summary (
            customer_id,
            customer_segment,
            region_id,
            total_daily_transactions,
            total_daily_volume_usd,
            max_single_transaction_usd,
            high_risk_mcc_count,
            unique_destinations_count,
            daily_risk_flag,
            summary_date -- BigQuery partition column part of the select list
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
            '{process_date}' AS summary_date -- Inject process_date directly
        FROM fin_core.fact_transactions -- Assuming fin_core is a BigQuery dataset
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

    # Hive CREATE DATABASE is equivalent to BigQuery CREATE SCHEMA (dataset).
    # SparkSQL with BigQuery connector allows CREATE DATABASE which creates a dataset.
    sp.sql("CREATE DATABASE IF NOT EXISTS fin_landing")
    sp.sql("CREATE DATABASE IF NOT EXISTS fin_core")
    sp.sql("CREATE DATABASE IF NOT EXISTS fin_mart")

    run_etl_pipeline(sp, p_date)
    sp.stop()
