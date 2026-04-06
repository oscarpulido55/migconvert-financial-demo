import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connector and settings."""
    # Removed Hive-specific configurations like 'enableHiveSupport' and 'hive.exec.dynamic.partition'.
    # Added BigQuery connector properties:
    # 'spark.jars.packages' specifies the Spark-BigQuery connector dependency.
    # 'parentProject' and 'temporaryGcsBucket' are typically required for BigQuery writes from Spark.
    # 'spark.cloud.google.credentials.file' (or GOOGLE_APPLICATION_CREDENTIALS env var) and
    # 'spark.hadoop.google.cloud.auth.service.account.enable' configure service account authentication.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_BigQuery_Embedded_SQL") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("parentProject", "your-gcp-project-id") \
        .config("temporaryGcsBucket", "your-gcs-bucket-for-temp-data") \
        .config("spark.cloud.google.credentials.file", "/path/to/your/service-account-key.json") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries via Spark.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # The HiveQL 'SET hiveconf:process_date='{process_date}' is not directly applicable to BigQuery.
    # The process_date variable is now directly embedded into SQL strings using Python f-strings,
    # ensuring it's treated as a BigQuery DATE type.

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
        WHERE DATE(transaction_timestamp) = DATE '{process_date}' -- Converted from HIVEQL's to_date() and '${hiveconf:process_date}' variable syntax
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # To achieve functional equivalence of Hive's 'INSERT OVERWRITE PARTITION (col)',
    # BigQuery for partitioned tables (e.g., partitioned by 'trx_date') typically requires
    # an explicit DELETE of existing data for the target partition, followed by an INSERT INTO for the new data.
    delete_fact_sql = f"""
        DELETE FROM fin_core.fact_transactions
        WHERE trx_date = DATE '{process_date}'
    """
    spark.sql(delete_fact_sql)

    insert_sql = f"""
        INSERT INTO fin_core.fact_transactions -- Converted from HIVEQL's 'INSERT OVERWRITE TABLE ... PARTITION (...)'
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS DECIMAL(18, 4)) AS normalized_usd_amount,
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
            
            -- Partitioning Columns (BigQuery expects the partitioning column, e.g., 'trx_date', to be part of the SELECT list)
            DATE(r.transaction_timestamp) AS trx_date, -- Converted from HIVEQL's to_date()
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN fin_core.dim_customers c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
          AND DATE(r.transaction_timestamp) = DATE '{process_date}' -- Ensures only relevant data for process_date is inserted, consistent with partition overwrite
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Similar to the fact table, using DELETE and INSERT to emulate 'INSERT OVERWRITE PARTITION' for the 'summary_date' partition.
    delete_risk_sql = f"""
        DELETE FROM fin_mart.risk_daily_summary
        WHERE summary_date = DATE '{process_date}'
    """
    spark.sql(delete_risk_sql)

    risk_sql = f"""
        INSERT INTO fin_mart.risk_daily_summary -- Converted from HIVEQL's 'INSERT OVERWRITE TABLE ... PARTITION (...)'
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
            DATE '{process_date}' AS summary_date -- Converted from HIVEQL's '${hiveconf:process_date}' variable
        FROM fin_core.fact_transactions
        WHERE trx_date = DATE '{process_date}' -- Converted from HIVEQL's '${hiveconf:process_date}' variable
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
    
    # In BigQuery, databases are referred to as 'datasets' or 'schemas'.
    # This command creates BigQuery datasets if they don't exist.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_landing") # Converted from HIVEQL's CREATE DATABASE
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_core")    # Converted from HIVEQL's CREATE DATABASE
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_mart")     # Converted from HIVEQL's CREATE DATABASE
    
    run_etl_pipeline(sp, p_date)
    sp.stop()