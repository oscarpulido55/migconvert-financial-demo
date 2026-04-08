import sys
import math
from google.cloud import bigquery

def create_bigquery_client():
    """Initializes and returns a BigQuery client."""
    return bigquery.Client()

# Create a custom UDF for great circle distance
def haversine(lat1, lon1, lat2, lon2):
    """Calculates the great circle distance between two points on the earth."""
    # This Python UDF logic will be translated into BigQuery SQL within the query string
    # for efficient execution on BigQuery data. The Python function is retained as per original.
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return -1.0
    R = 6371.0 # Radius of earth in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def score_transactions_for_fraud(client, execution_date, project_id):
    """
    Uses BigQuery SQL with CTEs to score credit card transactions.
    Replaces PySpark DataFrame APIs with continuous BigQuery SQL transformations.
    """
    
    # All PySpark DataFrame steps (Load, Filter, Join, WithColumn, GroupBy, Agg, Window)
    # are translated into a single BigQuery SQL query using Common Table Expressions (CTEs).
    # The 'haversine_udf' functionality is implemented directly using BigQuery's
    # SQL math functions (Spherical Law of Cosines is functionally equivalent to Haversine).
    bq_sql_query = f"""
    -- 1. Load Data & 2. Extract current day transactions
    WITH current_day_transactions AS (
        SELECT *
        FROM `{project_id}.fin_core.cc_transactions`
        WHERE trx_date = PARSE_DATE('%Y-%m-%d', '{execution_date}')
    ),
    
    -- 3. Join location data and calculate distance using BigQuery SQL functions
    enriched_transactions AS (
        SELECT
            t.* EXCEPT(merchant_id, account_id), -- Exclude joined-on columns to avoid duplicates or issues
            t.account_id,
            t.merchant_id,
            a.home_lat,
            a.home_lon,
            m.merchant_lat,
            m.merchant_lon,
            -- BigQuery SQL equivalent of haversine calculation (Spherical Law of Cosines)
            -- COALESCE handles cases where any lat/lon is NULL, resulting in a NULL distance, setting it to -1.0
            COALESCE(
                6371.0 * ACOS(
                    COS(RADIANS(a.home_lat)) * COS(RADIANS(m.merchant_lat)) * COS(RADIANS(m.merchant_lon) - RADIANS(a.home_lon)) +
                    SIN(RADIANS(a.home_lat)) * SIN(RADIANS(m.merchant_lat))
                ),
                -1.0
            ) AS distance_from_home_km
        FROM current_day_transactions AS t
        INNER JOIN `{project_id}.fin_core.dim_accounts` AS a
            ON t.account_id = a.account_id
        LEFT JOIN `{project_id}.fin_core.dim_merchants` AS m
            ON t.merchant_id = m.merchant_id
    ),
    
    -- 4. Pure BigQuery SQL Historical Profiling (90 days prior to execution_date)
    historical_transactions AS (
        SELECT
            account_id,
            amount,
            trx_id
        FROM `{project_id}.fin_core.cc_transactions`
        WHERE
            trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', '{execution_date}'), INTERVAL 90 DAY)
            AND trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', '{execution_date}'), INTERVAL 1 DAY)
    ),
    
    historical_profile AS (
        SELECT
            account_id,
            AVG(amount) AS avg_trx_amount_90d,
            -- COALESCE handles cases where STDDEV_SAMP returns NULL (e.g., only one transaction), defaulting to 0.0
            COALESCE(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d,
            COUNT(trx_id) / 90.0 AS avg_daily_trx_count
        FROM historical_transactions
        GROUP BY account_id
    ),
    
    -- 5. Join current day transactions with their historical profiles
    features_joined AS (
        SELECT
            curr.*, -- Select all columns from enriched_transactions
            hist.avg_trx_amount_90d,
            hist.stddev_trx_amount_90d,
            hist.avg_daily_trx_count
        FROM enriched_transactions AS curr
        LEFT JOIN historical_profile AS hist
            ON curr.account_id = hist.account_id
    ),
    
    -- 6. Apply Time-based Window Function & 7. Apply Complex Business Logic (partial)
    scored_df_intermediate AS (
        SELECT
            *,
            -- trx_last_hour_cnt: BigQuery window function for "last hour transactions count"
            -- UNIX_SECONDS ensures a precise time-based window in seconds
            COUNT(trx_id) OVER (
                PARTITION BY account_id
                ORDER BY UNIX_SECONDS(trx_timestamp)
                RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW -- Equivalent to PySpark's rangeBetween(-3600, 0)
            ) AS trx_last_hour_cnt,
            -- amount_z_score calculation
            CASE
                WHEN stddev_trx_amount_90d > 0 THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d
                ELSE 0.0
            END AS amount_z_score
        FROM features_joined
    ),
    
    -- Final calculation of risk scores and total fraud score
    final_scores_calculated AS (
        SELECT
            *,
            -- distance_risk_score
            CASE
                WHEN (distance_from_home_km > 500 AND distance_from_home_km != -1.0) THEN 30
                ELSE 0
            END AS distance_risk_score,
            -- amount_risk_score
            CASE
                WHEN amount_z_score > 3.0 THEN 40
                WHEN amount_z_score > 2.0 THEN 20
                ELSE 0
            END AS amount_risk_score,
            -- velocity_risk_score
            CASE
                WHEN trx_last_hour_cnt > 5 THEN 30
                WHEN trx_last_hour_cnt > 3 THEN 15
                ELSE 0
            END AS velocity_risk_score
        FROM scored_df_intermediate
    )
    
    -- 8. Select final columns and prepare for insertion
    SELECT
        trx_id,
        account_id,
        amount,
        distance_from_home_km,
        amount_z_score,
        trx_last_hour_cnt,
        (distance_risk_score + amount_risk_score + velocity_risk_score) AS fraud_score,
        (distance_risk_score + amount_risk_score + velocity_risk_score) >= 60 AS is_fraud_alert,
        PARSE_DATE('%Y-%m-%d', '{execution_date}') AS scoring_date
    FROM final_scores_calculated
    """
    
    # 8. Execute INSERT INTO target table
    insert_target_table = f"`{project_id}.fin_mart.fraud_scores_daily`"
    
    final_insert_query = f"""
    INSERT INTO {insert_target_table} (
        trx_id, account_id, amount, distance_from_home_km,
        amount_z_score, trx_last_hour_cnt, fraud_score, is_fraud_alert, scoring_date
    )
    {bq_sql_query}
    """
    
    job = client.query(final_insert_query)
    job.result() # Waits for the job to complete
    
    print(f"BigQuery Fraud scoring completed for {execution_date}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python fin_credit_card_fraud_scoring.py <YYYY-MM-DD> <GCP_PROJECT_ID>")
        sys.exit(1)
        
    exec_date = sys.argv[1]
    gcp_project_id = sys.argv[2]
    bq_client = create_bigquery_client()
    
    score_transactions_for_fraud(bq_client, exec_date, gcp_project_id)
    bq_client.close()