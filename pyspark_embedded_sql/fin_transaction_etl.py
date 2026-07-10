import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connection."""
    # Removed Hive specific configurations and added BigQuery connector configurations.
    # Note: Replace "your-gcs-bucket-name" and "/path/to/your/gcp_keyfile.json" with actual values.
    # The 'spark.jars.packages' specifies the BigQuery connector.
    # 'temporaryGcsBucket' is crucial for Spark to stage data when interacting with BigQuery.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/your/gcp_keyfile.json") \
        .config("temporaryGcsBucket", "your-gcs-bucket-name") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # Hive-specific 'SET hiveconf:process_date' is removed.
    # In BigQuery, parameters are directly substituted into the SQL query string
    # or passed through Spark's query parameters (though direct f-string is used here).
    # spark.sql(f"SET hiveconf:process_date='{process_date}'") # REMOVED

    # 1. Create temporary view for the delta transactions from daily landing zone
    logger.info("Creating temporary view for raw transactions...")
    # Converted Hive's to_date() to BigQuery's DATE() function.
    # Replaced '${hiveconf:process_date}' with Python f-string '{process_date}'.
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
        WHERE DATE(transaction_timestamp) = '{process_date}'
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # Removed Hive-specific 'PARTITION (trx_date, region_id)' from INSERT statement.
    # BigQuery table partitioning is defined in its DDL, not during INSERT.
    # 'INSERT OVERWRITE' via Spark's BigQuery connector typically overwrites partitions
    # corresponding to the data being inserted, if the target table is partitioned by the output columns.
    # Changed DECIMAL to NUMERIC for explicit BigQuery type compatibility.
    insert_sql = f"""
        INSERT OVERWRITE fin_core.fact_transactions
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
    # Removed Hive-specific 'PARTITION (summary_date)' from INSERT statement.
    # Replaced '${hiveconf:process_date}' with Python f-string '{process_date}'.
    risk_sql = f"""
        INSERT OVERWRITE fin_mart.risk_daily_summary
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
            '{process_date}' AS summary_date
        FROM fin_core.fact_transactions
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

    # 'CREATE DATABASE' in Spark SQL is often translated to 'CREATE DATASET' in BigQuery
    # by the BigQuery connector. Ensure your GCP project has necessary permissions.
    sp.sql("CREATE DATABASE IF NOT EXISTS fin_landing")
    sp.sql("CREATE DATABASE IF NOT EXISTS fin_core")
    sp.sql("CREATE DATABASE IF NOT EXISTS fin_mart")

    run_etl_pipeline(sp, p_date)
    sp.stop()
