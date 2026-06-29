import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connector support."""
    # Ensure you have downloaded the BigQuery connector JAR or specified it in spark-submit's --packages
    # Example for --packages: --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0
    # The version 0.29.0 should be compatible with Spark 3.x and Scala 2.12. Adjust if needed.
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL_BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0") \
        .config("spark.datasource.bigquery.temporaryGcsBucket", "your-gcs-temp-bucket") \
        .config("spark.sql.extensions", "com.google.cloud.spark.bigquery.BigQuerySparkSessionExtension") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using complex embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # No direct BigQuery equivalent for SET hiveconf.
    # The process_date parameter will be directly injected into SQL using Python f-strings.

    # Define BigQuery project ID. Replace with your actual GCP project ID.
    project_id = "your_gcp_project_id"

    # 1. Create temporary view for the delta transactions from daily landing zone
    logger.info("Creating temporary view for raw transactions...")
    # Assuming `your_gcp_project_id.fin_landing.raw_transactions` is your BigQuery table.
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
        FROM `{project_id}.fin_landing.raw_transactions`
        WHERE CAST(transaction_timestamp AS DATE) = '{process_date}'
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # Assuming `your_gcp_project_id.fin_core.fact_transactions` is a BigQuery table,
    # and `trx_date` and `region_id` are partitioning columns defined during table creation in BigQuery.
    # Spark's BigQuery connector will handle writing to the correct partitions.
    # `INSERT OVERWRITE TABLE` in Spark with BigQuery connector typically means overwriting entire table or specific partitions.
    insert_sql = f"""
        INSERT OVERWRITE `{project_id}.fin_core.fact_transactions`
        SELECT
            r.trx_uuid,
            r.source_account_id,
            r.destination_account_id,
            r.transaction_type,
            r.amount_base_currency,
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC(38, 9)) AS normalized_usd_amount,
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

            -- Partitioning Columns. These columns must exist in the target BigQuery table's schema
            CAST(r.transaction_timestamp AS DATE) AS trx_date,
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `{project_id}.fin_core.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN `{project_id}.fin_core.dim_customers` c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # Assuming `your_gcp_project_id.fin_mart.risk_daily_summary` is a BigQuery table partitioned by `summary_date`.
    risk_sql = f"""
        INSERT OVERWRITE `{project_id}.fin_mart.risk_daily_summary`
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
        FROM `{project_id}.fin_core.fact_transactions`
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

    # BigQuery uses 'datasets' instead of 'databases'.
    # In Spark, CREATE DATABASE usually maps to creating a BigQuery dataset if the connector is configured.
    # If the datasets do not exist, they would need to be created manually or via BigQuery client API/gcloud.
    # The following commands will attempt to create them via Spark SQL on BigQuery.
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_landing") # Creates a dataset named fin_landing in the default project
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_core") # Creates a dataset named fin_core in the default project
    sp.sql("CREATE SCHEMA IF NOT EXISTS fin_mart") # Creates a dataset named fin_mart in the default project

    run_etl_pipeline(sp, p_date)
    sp.stop()
