import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connection and other configurations."""
    # Major Change: Removed Hive-specific configurations and added BigQuery specific configurations.
    # Required Configuration: 'your_gcp_project_id' should be replaced with your actual Google Cloud Project ID.
    # Required Configuration: The 'spark-bigquery-with-dependencies_2.12' package version (0.29.0) might need to be adjusted
    # based on your Spark and Scala versions for compatibility.
    # Required Configuration: Depending on your execution environment (e.g., local, GCE, Dataproc),
    # additional authentication setup (e.g., service account keyfile path) may be required.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL_BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.cloud.google.project.id", "your_gcp_project_id") \
        .config("spark.hadoop.google.cloud.project.id", "your_gcp_project_id") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded Spark SQL queries for BigQuery.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # Major Change: Removed Hive-specific `SET hiveconf` as BigQuery uses different parameter handling.
    # The 'process_date' will now be directly interpolated into the SQL strings using f-strings.
    # spark.sql(f"SET hiveconf:process_date='{process_date}'")

    # 1. Create temporary view for the delta transactions from daily landing zone
    logger.info("Creating temporary view for raw transactions...")
    # Major Change: Table references updated to BigQuery format: project.dataset.table
    # Major Change: Date comparison for BigQuery compatibility using `CAST(timestamp_column AS DATE)`.
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
        FROM your_gcp_project_id.fin_landing.raw_transactions
        WHERE CAST(transaction_timestamp AS DATE) = '{process_date}'
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info(f"Inserting data into your_gcp_project_id.fin_core.fact_transactions for {process_date}...")
    insert_sql = f"""
        INSERT OVERWRITE TABLE your_gcp_project_id.fin_core.fact_transactions -- Major Change: Table name updated to project.dataset.table.
                                                                           -- Minor Change: Removed `PARTITION (...)` clause.
                                                                           -- Spark's BigQuery connector handles dynamic partition writes
                                                                           -- when the target table is partitioned in BigQuery,
                                                                           -- by identifying partition columns in the SELECT statement.
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC) AS normalized_usd_amount, -- Major Change: Replaced DECIMAL(18, 4) with BigQuery's NUMERIC type.
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
            
            -- Partitioning Columns - ensure these are defined as partition columns in the BigQuery table schema
            CAST(r.transaction_timestamp AS DATE) AS trx_date, -- Major Change: Using `CAST(... AS DATE)` for BigQuery compatibility.
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN your_gcp_project_id.fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id -- Major Change: Table reference updated to project.dataset.table
        LEFT JOIN your_gcp_project_id.fin_core.dim_customers c ON dim_a.customer_id = c.customer_id -- Major Change: Table reference updated to project.dataset.table
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    risk_sql = f"""
        INSERT OVERWRITE TABLE your_gcp_project_id.fin_mart.risk_daily_summary -- Major Change: Table name updated to project.dataset.table.
                                                                            -- Minor Change: Removed `PARTITION (...)` clause.
        SELECT
            customer_id,
            customer_segment,
            region_id,
            COUNT(trx_uuid) AS total_daily_transactions,
            SUM(normalized_usd_amount) AS total_daily_volume_usd,
            MAX(normalized_usd_amount) AS max_single_transaction_usd,
            COUNTIF(merchant_category_code IN ('7995', '6012')) AS high_risk_mcc_count, -- Major Change: Using BigQuery-specific `COUNTIF` for conciseness.
            COUNT(DISTINCT destination_account_id) AS unique_destinations_count,
            -- Flag for Review
            CASE 
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW' 
            END AS daily_risk_flag,
            '{process_date}' AS summary_date -- Major Change: Directly interpolating process_date instead of using '${hiveconf:process_date}'.
        FROM your_gcp_project_id.fin_core.fact_transactions -- Major Change: Table reference updated to project.dataset.table
        WHERE trx_date = '{process_date}' -- Major Change: Directly interpolating process_date.
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
    
    # Major Change: In BigQuery, "databases" are conceptually "datasets". `CREATE SCHEMA` in Spark SQL translates to creating a BigQuery dataset.
    # Required Configuration: 'your_gcp_project_id' must be the actual project ID as BigQuery dataset names are scoped under a project.
    # Compatibility Issue: While `CREATE SCHEMA` is used here, it's often more robust to ensure BigQuery datasets exist
    # beforehand using gcloud CLI or client libraries outside the Spark job.
    sp.sql("CREATE SCHEMA IF NOT EXISTS your_gcp_project_id.fin_landing")
    sp.sql("CREATE SCHEMA IF NOT EXISTS your_gcp_project_id.fin_core")
    sp.sql("CREATE SCHEMA IF NOT EXISTS your_gcp_project_id.fin_mart")
    
    run_etl_pipeline(sp, p_date)
    sp.stop()