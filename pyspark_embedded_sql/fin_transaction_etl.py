import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session."""
    # Removed enableHiveSupport() and Hive-specific configs for BigQuery compatibility.
    # Spark interacts with BigQuery via the Spark-BigQuery connector.
    # Further BigQuery-specific configurations (e.g., spark.jars, authentication)
    # would typically be set up in the execution environment (e.g., Dataproc) or
    # explicitly via .config() lines not present in the original input.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries within Spark.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # The Hive-specific 'SET hiveconf:process_date' is removed.
    # For BigQuery, parameters are typically interpolated directly into the SQL string.

    # 1. Create temporary view for the delta transactions from daily landing zone
    logger.info("Creating temporary view for raw transactions...")
    # Converted table name to BigQuery format (`project.dataset.table`) and updated date function.
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
        FROM `your_gcp_project_id.fin_landing.raw_transactions`
        WHERE DATE(transaction_timestamp) = DATE('{process_date}')
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into `your_gcp_project_id.fin_core.fact_transactions`...")
    # Converted table names to BigQuery format.
    # Removed the Hive-specific 'PARTITION (trx_date, region_id)' clause.
    # When using Spark's BigQuery connector with `INSERT OVERWRITE`, partitioning behavior
    # is usually managed by the BigQuery table's schema definition and the connector's write mode.
    # Converted DECIMAL(18, 4) to BIGNUMERIC for BigQuery high-precision decimal type.
    insert_sql = f"""
        INSERT OVERWRITE `your_gcp_project_id.fin_core.fact_transactions`
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
            -- Rolling sum (BigQuery compatible window function syntax)
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,
            
            -- Partitioning Columns (ensure BigQuery table is partitioned on these)
            DATE(r.transaction_timestamp) AS trx_date,
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `your_gcp_project_id.fin_core.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN `your_gcp_project_id.fin_core.dim_customers` c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Converted table names to BigQuery format.
    # Removed the Hive-specific 'PARTITION (summary_date)' clause.
    # Replaced '${hiveconf:process_date}' with Python f-string interpolation for date.
    risk_sql = f"""
        INSERT OVERWRITE `your_gcp_project_id.fin_mart.risk_daily_summary`
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
            DATE('{process_date}') AS summary_date
        FROM `your_gcp_project_id.fin_core.fact_transactions`
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
    
    # Initialize Datasets for safety
    # Converted 'DATABASE' to BigQuery's 'SCHEMA' (alias for DATASET) and added project ID.
    sp.sql("CREATE SCHEMA IF NOT EXISTS `your_gcp_project_id.fin_landing`")
    sp.sql("CREATE SCHEMA IF NOT EXISTS `your_gcp_project_id.fin_core`")
    sp.sql("CREATE SCHEMA IF NOT EXISTS `your_gcp_project_id.fin_mart`")
    
    run_etl_pipeline(sp, p_date)
    sp.stop()