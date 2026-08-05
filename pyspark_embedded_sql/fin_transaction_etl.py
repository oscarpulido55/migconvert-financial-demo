import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session with BigQuery connector and settings."""
    # <COMMENT> Configure your GCP Project ID and service account if not using default credentials. </COMMENT>
    # Set your GCP Project ID here
    gcp_project_id = "your_gcp_project_id" # <COMMENT> REPLACE WITH YOUR ACTUAL GCP PROJECT ID </COMMENT>
    # Path to your BigQuery service account key JSON file if not relying on Application Default Credentials (ADC)
    # E.g., if running outside of GCP or explicitly managing keys.
    # bigquery_key_file = "/path/to/your/service_account_key.json" # <COMMENT> UNCOMMENT AND REPLACE IF USING SERVICE ACCOUNT KEY FILE </COMMENT>

    spark_builder = SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL_BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") # <COMMENT> Recommended for compatibility with older date formats in Spark 3.x+. </COMMENT>

    # <COMMENT> BigQuery connector requires the GCP project ID. </COMMENT>
    spark_builder = spark_builder.config("spark.hadoop.google.cloud.project.id", gcp_project_id)
    # <COMMENT> If using a service account JSON key file directly, configure it here. Otherwise, Spark will
    #          try to use Application Default Credentials. </COMMENT>
    # spark_builder = spark_builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", bigquery_key_file)

    # <COMMENT> Removed Hive-specific configurations as they are not applicable to BigQuery,
    #          e.g., .enableHiveSupport() and hive.exec.dynamic.partition.* settings.
    #          BigQuery handles partitioning implicitly when writing to partitioned tables. </COMMENT>

    return spark_builder.getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    # <COMMENT> Retrieve GCP Project ID from Spark configuration. </COMMENT>
    gcp_project_id = spark.sparkContext.getConf().get("spark.hadoop.google.cloud.project.id")
    if not gcp_project_id:
        raise ValueError("GCP Project ID not configured in SparkSession. Please set 'spark.hadoop.google.cloud.project.id'.")

    # <COMMENT> Hiveconf variable setting is replaced by direct Python f-string interpolation into BigQuery SQL. </COMMENT>
    # spark.sql(f"SET hiveconf:process_date='{process_date}'") # Removed as BigQuery does not use hiveconf

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
        FROM `{gcp_project_id}.fin_landing.raw_transactions` -- <COMMENT> Fully qualified table name for BigQuery: `project.dataset.table`. </COMMENT>
        WHERE DATE(transaction_timestamp) = DATE('{process_date}') -- <COMMENT> `to_date()` for HiveQL is replaced with BigQuery's `DATE()` function. </COMMENT>
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info("Inserting data into fin_core.fact_transactions...")
    # <COMMENT> BigQuery's SQL for overwriting specific partitions differs from Hive's `INSERT OVERWRITE TABLE PARTITION`.
    #          The functional equivalent typically involves two steps: first deleting the data for the target partition(s),
    #          then inserting the new data. Ensure `fin_core.fact_transactions` is a partitioned table in BigQuery,
    #          e.g., partitioned by `trx_date` (DATE type) with `region_id` as a clustering key. </COMMENT>

    # Delete existing data for the process_date partition to simulate 'overwrite' behavior
    spark.sql(f"""
        DELETE FROM `{gcp_project_id}.fin_core.fact_transactions`
        WHERE trx_date = DATE('{process_date}')
    """) # <COMMENT> Replaced Hive's `INSERT OVERWRITE TABLE` with explicit `DELETE` statement. </COMMENT>

    insert_sql = f"""
        INSERT INTO `{gcp_project_id}.fin_core.fact_transactions` (
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
            -- Calculate normalized amount for aggregations. BIGNUMERIC provides higher precision than NUMERIC.
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC(38, 4)) AS normalized_usd_amount, -- <COMMENT> Hive's `DECIMAL` is replaced with BigQuery's `BIGNUMERIC` for equivalent precision. </COMMENT>
            r.currency_code,
            r.transaction_timestamp,
            r.merchant_category_code,
            r.channel,
            c.customer_id,
            c.customer_segment,
            c.kyc_status,
            -- Rolling sum window function is compatible with BigQuery.
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,

            -- Partitioning Columns. trx_date is typically the partition key, region_id can be a clustering key.
            DATE(r.transaction_timestamp) AS trx_date, -- <COMMENT> `to_date()` for HiveQL is replaced with BigQuery's `DATE()` function. </COMMENT>
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `{gcp_project_id}.fin_core.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id -- <COMMENT> Fully qualified table name. </COMMENT>
        LEFT JOIN `{gcp_project_id}.fin_core.dim_customers` c ON dim_a.customer_id = c.customer_id -- <COMMENT> Fully qualified table name. </COMMENT>
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql) # <COMMENT> Insert new data for the specific date after deletion. </COMMENT>

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")
    # <COMMENT> Similar to `fact_transactions`, `fin_mart.risk_daily_summary` should be partitioned
    #          by `summary_date` (DATE type) in BigQuery to support the DELETE + INSERT pattern. </COMMENT>

    # Delete existing data for the process_date partition
    spark.sql(f"""
        DELETE FROM `{gcp_project_id}.fin_mart.risk_daily_summary`
        WHERE summary_date = DATE('{process_date}')
    """) # <COMMENT> Replaced Hive's `INSERT OVERWRITE TABLE` with explicit `DELETE` statement. </COMMENT>

    risk_sql = f"""
        INSERT INTO `{gcp_project_id}.fin_mart.risk_daily_summary` (
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
            COUNTIF(merchant_category_code IN ('7995', '6012')) AS high_risk_mcc_count, -- <COMMENT> Hive's `COUNT(CASE WHEN ... THEN 1 END)` replaced with BigQuery's `COUNTIF()`. </COMMENT>
            COUNT(DISTINCT destination_account_id) AS unique_destinations_count,
            -- Flag for Review logic is compatible.
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            DATE('{process_date}') AS summary_date -- <COMMENT> Hiveconf variable `${hiveconf:process_date}` and string literal for date replaced with `DATE()` function. </COMMENT>
        FROM `{gcp_project_id}.fin_core.fact_transactions` -- <COMMENT> Fully qualified table name. </COMMENT>
        WHERE trx_date = DATE('{process_date}') -- <COMMENT> Date filter uses BigQuery's `DATE()` function. </COMMENT>
        GROUP BY
            customer_id,
            customer_segment,
            region_id
    """
    spark.sql(risk_sql) # <COMMENT> Insert new data for the specific date after deletion. </COMMENT>

    logger.info("Successfully completed ETL pipeline.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fin_transaction_etl.py <YYYY-MM-DD>")
        sys.exit(1)

    p_date = sys.argv[1]
    sp = create_spark_session()

    # <COMMENT> BigQuery datasets (`fin_landing`, `fin_core`, `fin_mart`) must be pre-created
    #          in your GCP project, as there's no direct SQL equivalent for `CREATE DATABASE`
    #          that creates BigQuery datasets via the Spark-BigQuery connector in spark.sql context. </COMMENT>
    # sp.sql("CREATE DATABASE IF NOT EXISTS fin_landing") # Removed
    # sp.sql("CREATE DATABASE IF NOT EXISTS fin_core") # Removed
    # sp.sql("CREATE DATABASE IF NOT EXISTS fin_mart") # Removed

    run_etl_pipeline(sp, p_date)
    sp.stop()
