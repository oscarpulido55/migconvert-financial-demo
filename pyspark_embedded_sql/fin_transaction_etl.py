import sys
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initializes Spark Session for BigQuery connection."""
    return SparkSession.builder \
        .appName("Financial_Transaction_ETL_Embedded_SQL_BigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.1") \
        .config("spark.hadoop.google.cloud.project.id", "migconvert-at-next26") \
        .getOrCreate()

def run_etl_pipeline(spark: SparkSession, process_date: str):
    """
    Executes the ETL pipeline using embedded BigQuery SQL queries.
    This demonstrates the capability of managing SQL directly within Python strings.
    """
    logger.info(f"Starting execution for process_date: {process_date}")

    project_id = "migconvert-at-next26"
    landing_dataset = "fin_landing"
    core_dataset = "fin_core"
    mart_dataset = "fin_mart"

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
        FROM `{project_id}.{landing_dataset}.raw_transactions`
        WHERE DATE(transaction_timestamp) = DATE('{process_date}')
    """)

    # 2. Complex ETL to fact table using embedded SQL
    logger.info(f"Inserting data into `{project_id}.{core_dataset}.fact_transactions`...")

    # BigQuery equivalent for INSERT OVERWRITE TABLE PARTITION is DELETE then INSERT
    # Assumes fact_transactions is partitioned by trx_date
    spark.sql(f"""
        DELETE FROM `{project_id}.{core_dataset}.fact_transactions`
        WHERE trx_date = DATE('{process_date}')
    """)

    insert_sql = f"""
        INSERT INTO `{project_id}.{core_dataset}.fact_transactions` (
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
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC) AS normalized_usd_amount,
            r.currency_code,
            r.transaction_timestamp,
            r.merchant_category_code,
            r.channel,
            c.customer_id,
            c.customer_segment,
            c.kyc_status,
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,
            DATE(r.transaction_timestamp) AS trx_date,
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN `{project_id}.{core_dataset}.dim_accounts` dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN `{project_id}.{core_dataset}.dim_customers` c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
    """
    spark.sql(insert_sql)

    # 3. Create Aggregated Datamart for Risk Analysis
    logger.info("Executing aggregation for Risk Datamart...")

    # BigQuery equivalent for INSERT OVERWRITE TABLE PARTITION is DELETE then INSERT
    # Assumes risk_daily_summary is partitioned by summary_date
    spark.sql(f"""
        DELETE FROM `{project_id}.{mart_dataset}.risk_daily_summary`
        WHERE summary_date = DATE('{process_date}')
    """)

    risk_sql = f"""
        INSERT INTO `{project_id}.{mart_dataset}.risk_daily_summary` (
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
            COUNTIF(merchant_category_code IN ('7995', '6012')) AS high_risk_mcc_count,
            COUNT(DISTINCT destination_account_id) AS unique_destinations_count,
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            DATE('{process_date}') AS summary_date
        FROM `{project_id}.{core_dataset}.fact_transactions`
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

    project_id = "migconvert-at-next26"

    # Create BigQuery datasets (equivalent to Hive databases/schemas)
    # Note: These operations require proper BigQuery permissions configured for the Spark environment
    sp.sql(f"CREATE SCHEMA IF NOT EXISTS `{project_id}.fin_landing`")
    sp.sql(f"CREATE SCHEMA IF NOT EXISTS `{project_id}.fin_core`")
    sp.sql(f"CREATE SCHEMA IF NOT EXISTS `{project_id}.fin_mart`")

    run_etl_pipeline(sp, p_date)
    sp.stop()
