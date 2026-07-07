from google.cloud import bigquery # Converted from `from pyspark.sql import SparkSession`
# PySpark functions replaced by BigQuery SQL equivalents within query strings (e.g., col, lit, current_timestamp, to_date, year, month, dayofmonth, max, sum, broadcast, when, coalesce, udf, array_contains, explode)
# PySpark Window class is replaced by SQL window functions within query strings
# PySpark SQL types (StringType, DoubleType, IntegerType, StructType, StructField, ArrayType) are handled by BigQuery's schema inference or explicit SQL DDL.

def get_bigquery_client(): # Converted from `get_spark_session`
    """Initializes and returns a BigQuery client.""" # Converted docstring
    return bigquery.Client() # Replaces `SparkSession.builder` and its chained methods.
    # .appName("Financial_Customer_Onboarding_ETL") # Spark configuration, not applicable for BigQuery client
    # .enableHiveSupport() # Spark-specific feature, not applicable
    # .config("spark.sql.sources.partitionOverwriteMode", "dynamic") # Spark configuration, not applicable
    # .config("spark.sql.adaptive.enabled", "true") # Spark configuration, not applicable
    # .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Spark configuration, not applicable
    # .getOrCreate() # Spark-specific, replaced by direct client instantiation

def process_customer_onboarding(client): # `client` parameter instead of `spark`
    """
    Main ETL process for customer onboarding.
    Reads raw customer data, KYC records, and initial funding details,
    performs complex transformations, and writes to the dimensions table.
    """
    # 1. Source tables are assumed to be existing BigQuery tables or external tables on GCS.
    # The original HDFS paths (e.g., "hdfs://namenode:8020/landing/fin/customers/")
    # are mapped to BigQuery table references (project.dataset.table).
    # Replace 'your-gcp-project-id' with your actual GCP Project ID.
    raw_customers_df_ref = "`your-gcp-project-id.fin_core.raw_customers`" # BigQuery table reference for raw customer data
    raw_kyc_df_ref = "`your-gcp-project-id.fin_core.raw_kyc`"             # BigQuery table reference for raw KYC data
    raw_accounts_df_ref = "`your-gcp-project-id.fin_core.raw_accounts`"   # BigQuery table reference for raw account data

    # 2. Extract latest KYC status - This logic is translated into a BigQuery SQL Common Table Expression (CTE).
    # This CTE performs the equivalent of Spark's Window functions and filtering.
    latest_kyc_cte_sql = f"""
    latest_kyc AS (
        SELECT
            customer_id,
            status AS kyc_status,
            risk_rating AS kyc_risk_rating,
            verification_date
        FROM (
            SELECT
                customer_id,
                status,
                risk_rating,
                verification_date,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY verification_date DESC) as rn
            FROM {raw_kyc_df_ref}
        )
        WHERE rn = 1
    )"""

    # 3. Aggregate Initial Funding - This logic is translated into a BigQuery SQL Common Table Expression (CTE).
    # This CTE performs the equivalent of Spark's filter, groupBy, and agg functions.
    funding_agg_cte_sql = f"""
    funding_agg AS (
        SELECT
            customer_id,
            SUM(initial_deposit) AS total_initial_deposit,
            MAX(open_date) AS last_account_open_date
        FROM {raw_accounts_df_ref}
        WHERE account_status = 'ACTIVE'
        GROUP BY customer_id
    )"""

    # 4. Join and Enrich and 5. Complex Transformations (Calculate customer segments and risk profiles)
    # These operations are combined into a single BigQuery SQL SELECT statement, joining the CTEs.
    final_select_statement_sql = f"""
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.ssn_hash AS national_id_hash,
        c.dob AS date_of_birth,
        c.address.country AS country_code,
        c.address.state AS state_code,
        COALESCE(k.kyc_status, 'PENDING') AS kyc_status,
        COALESCE(k.kyc_risk_rating, 'UNKNOWN') AS risk_rating,
        COALESCE(f.total_initial_deposit, 0.0) AS total_initial_deposit,
        f.last_account_open_date,
        CURRENT_TIMESTAMP() AS etl_insert_ts,
        CASE
            WHEN COALESCE(f.total_initial_deposit, 0.0) > 1000000 THEN 'PRIVATE_WEALTH'
            WHEN (COALESCE(f.total_initial_deposit, 0.0) >= 100000) AND (COALESCE(f.total_initial_deposit, 0.0) <= 1000000) THEN 'PREMIUM'
            WHEN k.kyc_status = 'REJECTED' THEN 'RESTRICTED'
            ELSE 'RETAIL'
        END AS customer_segment,
        EXTRACT(YEAR FROM SAFE_CAST(f.last_account_open_date AS DATE)) AS onboarding_year,
        EXTRACT(MONTH FROM SAFE_CAST(f.last_account_open_date AS DATE)) AS onboarding_month
    FROM {raw_customers_df_ref} AS c
    LEFT JOIN latest_kyc AS k ON c.customer_id = k.customer_id
    LEFT JOIN funding_agg AS f ON c.customer_id = f.customer_id
    """

    # 6. Write to Managed BigQuery Table
    # The target is fin_core.dim_customers, partitioned by onboarding_year, onboarding_month, country_code.
    # In BigQuery, CREATE OR REPLACE TABLE AS SELECT is used for overwrite.
    # Partitioning is done using RANGE_BUCKET for integer year, and CLUSTER BY for month/country.
    target_table_id = "`your-gcp-project-id.fin_core.dim_customers`"

    # Combined SQL query for table creation and data insertion with transformations.
    final_bq_query = f"""
    CREATE OR REPLACE TABLE {target_table_id}
    PARTITION BY
        RANGE_BUCKET(onboarding_year, GENERATE_ARRAY(2000, 2100, 1)) -- Partitions for integer year
    CLUSTER BY onboarding_month, country_code -- Clustering for remaining partition keys
    AS
    WITH
        {latest_kyc_cte_sql},
        {funding_agg_cte_sql}
    {final_select_statement_sql}
    """

    client.query(final_bq_query).result() # Execute the BigQuery DDL/DML statement.

    # To replicate the log message's functional behavior (counting processed records).
    # Spark's count() is on DataFrame; BigQuery needs a separate query on the result table.
    count_query = f"SELECT count(*) FROM {target_table_id}"
    count_job = client.query(count_query)
    record_count = count_job.result().to_dataframe().iloc[0, 0] # Fetches the first (and only) count result.
    print(f"Successfully processed {record_count} customer records.")

if __name__ == "__main__":
    client = get_bigquery_client()

    # Optional: Setup DB for the demo
    # spark.sql("CREATE DATABASE IF NOT EXISTS fin_core")
    # BigQuery equivalent: CREATE SCHEMA IF NOT EXISTS <project_id>.fin_core
    # Using client.project for project_id and 'fin_core' as the dataset name.
    project_id_from_client = client.project # Get default project ID
    dataset_name = "fin_core"
    create_dataset_sql = f"CREATE SCHEMA IF NOT EXISTS `{project_id_from_client}.{dataset_name}` OPTIONS(location='US')"
    client.query(create_dataset_sql).result()

    process_customer_onboarding(client)
    # spark.stop() is a Spark-specific resource management call; not applicable to stateless BigQuery client.
