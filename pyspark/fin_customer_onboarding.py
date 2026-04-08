from google.cloud import bigquery

def process_customer_onboarding(client):
    """
    Main ETL process for customer onboarding.
    Reads raw customer data, KYC records, and initial funding details from BigQuery tables,
    performs complex transformations using BigQuery SQL, and writes to the dimensions table.
    """
    raw_customers_bq_table = "your_gcp_project_id.landing_fin.customers"
    raw_kyc_bq_table = "your_gcp_project_id.landing_fin.kyc_status"
    raw_accounts_bq_table = "your_gcp_project_id.landing_fin.accounts"

    dim_customers_bq_table = "your_gcp_project_id.fin_core.dim_customers"

    final_dim_customers_query = f"""
        WITH latest_kyc_cte AS (
            SELECT
                customer_id,
                status AS kyc_status,
                risk_rating AS kyc_risk_rating,
                verification_date,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY verification_date DESC) AS rn
            FROM `{raw_kyc_bq_table}`
        ),
        filtered_kyc AS (
            SELECT
                customer_id,
                kyc_status,
                kyc_risk_rating
            FROM latest_kyc_cte
            WHERE rn = 1
        ),
        funding_agg_cte AS (
            SELECT
                customer_id,
                SUM(initial_deposit) AS total_initial_deposit,
                MAX(CAST(open_date AS DATE)) AS last_account_open_date
            FROM `{raw_accounts_bq_table}`
            WHERE account_status = 'ACTIVE'
            GROUP BY customer_id
        )
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
                WHEN COALESCE(f.total_initial_deposit, 0.0) >= 100000 AND COALESCE(f.total_initial_deposit, 0.0) <= 1000000 THEN 'PREMIUM'
                WHEN COALESCE(k.kyc_status, 'PENDING') = 'REJECTED' THEN 'RESTRICTED'
                ELSE 'RETAIL'
            END AS customer_segment,
            EXTRACT(YEAR FROM f.last_account_open_date) AS onboarding_year,
            EXTRACT(MONTH FROM f.last_account_open_date) AS onboarding_month
        FROM `{raw_customers_bq_table}` AS c
        LEFT OUTER JOIN filtered_kyc AS k ON c.customer_id = k.customer_id
        LEFT OUTER JOIN funding_agg_cte AS f ON c.customer_id = f.customer_id
    """

    job_config = bigquery.QueryJobConfig(
        destination=dim_customers_bq_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="last_account_open_date"
        ),
        clustering_fields=["onboarding_year", "onboarding_month", "country_code"]
    )
    query_job = client.query(final_dim_customers_query, job_config=job_config)
    query_job.result()

    count_query = f"SELECT count(*) FROM `{dim_customers_bq_table}`"
    count_result = client.query(count_query).result()
    num_records = [row[0] for row in count_result][0]

    print(f"Successfully processed {num_records} customer records.")

if __name__ == "__main__":
    client = bigquery.Client()
    
    dataset_id = "fin_core"
    try:
        client.get_dataset(f"{client.project}.{dataset_id}")
    except bigquery.exceptions.NotFound:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset, timeout=30)
    
    process_customer_onboarding(client)