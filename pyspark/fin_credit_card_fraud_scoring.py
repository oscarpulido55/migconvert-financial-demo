import sys
import math
from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

# PySpark-specific imports (SparkSession, UDF functions, types, window) are removed.
# The functionality is replaced by direct BigQuery client operations and SQL query.

# The `create_spark_session` function is removed as BigQuery operations do not use SparkSession.

# The Python UDF `haversine` and its registration `haversine_udf` are replaced.
# The haversine calculation is embedded directly in the BigQuery SQL query as an expression.

def score_transactions_for_fraud(client, execution_date): # `spark` parameter replaced by `client`
    """
    Scores credit card transactions using BigQuery SQL.
    Replaces PySpark DataFrame API with a single, comprehensive BigQuery SQL query.
    """

    query = f"""
        WITH
        -- 1. Data Loading (Conceptual: BigQuery tables are referenced directly)

        -- 2. Extract current day transactions
        cc_trx AS (
            SELECT *
            FROM `fin_core.cc_transactions`
            WHERE trx_date = PARSE_DATE('%Y-%m-%d', @execution_date)
        ),

        -- 3. Join location data and calculate distance
        enriched_trx AS (
            SELECT
                t.* EXCEPT (home_lat, home_lon, merchant_lat, merchant_lon), -- Exclude temporary join columns to avoid conflict
                a.home_lat,
                a.home_lon,
                m.merchant_lat,
                m.merchant_lon,
                -- Haversine formula directly translated into BigQuery SQL expression
                CASE
                    WHEN a.home_lat IS NULL OR a.home_lon IS NULL OR m.merchant_lat IS NULL OR m.merchant_lon IS NULL THEN -1.0
                    ELSE
                        (
                            SELECT 2 * 6371.0 * ATAN2(SQRT(a_val), SQRT(1 - a_val))
                            FROM UNNEST([STRUCT(
                                POWER(SIN(RADIANS(m.merchant_lat - a.home_lat)/2), 2) +
                                COS(RADIANS(a.home_lat)) * COS(RADIANS(m.merchant_lat)) *
                                POWER(SIN(RADIANS(m.merchant_lon - a.home_lon)/2), 2) AS a_val
                            )])
                        )
                END AS distance_from_home_km
            FROM cc_trx AS t
            INNER JOIN `fin_core.dim_accounts` AS a ON t.account_id = a.account_id
            LEFT JOIN `fin_core.dim_merchants` AS m ON t.merchant_id = m.merchant_id
        ),

        -- 4. Historical Profiling
        hist_trx AS (
            SELECT *
            FROM `fin_core.cc_transactions`
            WHERE
                trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 90 DAY)
                AND trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 1 DAY)
        ),
        hist_profile AS (
            SELECT
                account_id,
                AVG(amount) AS avg_trx_amount_90d,
                COALESCE(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d, -- COALESCE replaces Spark's fillna
                COUNT(trx_id) / 90.0 AS avg_daily_trx_count
            FROM hist_trx
            GROUP BY account_id
        ),

        -- 5. Join current day transactions with their historical profiles
        df_features AS (
            SELECT
                curr.*,
                hist.avg_trx_amount_90d,
                hist.stddev_trx_amount_90d,
                hist.avg_daily_trx_count
            FROM enriched_trx AS curr
            LEFT JOIN hist_profile AS hist ON curr.account_id = hist.account_id
        ),

        -- 6 & 7. Apply Window Function, Business Logic and Scoring
        -- First CTE to calculate window functions and Z-score using BigQuery SQL functions
        scored_df_intermediate AS (
            SELECT
                df_features.*,
                COUNT(df_features.trx_id) OVER (
                    PARTITION BY df_features.account_id
                    ORDER BY UNIX_SECONDS(df_features.trx_timestamp) ASC
                    RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW -- 3600 seconds for 1 hour window
                ) AS trx_last_hour_cnt,
                CASE
                    WHEN df_features.stddev_trx_amount_90d > 0 THEN (df_features.amount - df_features.avg_trx_amount_90d) / df_features.stddev_trx_amount_90d
                    ELSE 0.0
                END AS amount_z_score
            FROM df_features
        ),

        -- Second CTE to calculate risk scores based on intermediate calculations
        final_scoring AS (
            SELECT
                scored_df_intermediate.*,
                CASE
                    WHEN distance_from_home_km > 500 AND distance_from_home_km != -1.0 THEN 30
                    ELSE 0
                END AS distance_risk_score,
                CASE
                    WHEN amount_z_score > 3.0 THEN 40
                    WHEN amount_z_score > 2.0 THEN 20
                    ELSE 0
                END AS amount_risk_score,
                CASE
                    WHEN trx_last_hour_cnt > 5 THEN 30
                    WHEN trx_last_hour_cnt > 3 THEN 15
                    ELSE 0
                END AS velocity_risk_score
            FROM scored_df_intermediate
        )

        -- 8. Insert results into target table. `insertInto` replaced by `INSERT INTO` SQL statement.
        INSERT INTO `fin_mart.fraud_scores_daily` (
            trx_id, account_id, amount,
            distance_from_home_km, amount_z_score, trx_last_hour_cnt,
            fraud_score, is_fraud_alert, scoring_date
        )
        SELECT
            trx_id,
            account_id,
            amount,
            distance_from_home_km,
            amount_z_score,
            trx_last_hour_cnt,
            (distance_risk_score + amount_risk_score + velocity_risk_score) AS fraud_score,
            (distance_risk_score + amount_risk_score + velocity_risk_score) >= 60 AS is_fraud_alert,
            PARSE_DATE('%Y-%m-%d', @execution_date) AS scoring_date
        FROM final_scoring;
    """

    # Configure and run the BigQuery job
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("execution_date", "STRING", execution_date),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result() # Waits for the job to complete

    print(f"BigQuery Fraud scoring completed for {execution_date}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fin_credit_card_fraud_scoring_bq.py <YYYY-MM-DD>")
        sys.exit(1)

    exec_date = sys.argv[1]
    # Initialize BigQuery client; this replaces `create_spark_session()`
    bq_client = bigquery.Client()

    score_transactions_for_fraud(bq_client, exec_date)
    # No explicit client `stop()` method needed for BigQuery
