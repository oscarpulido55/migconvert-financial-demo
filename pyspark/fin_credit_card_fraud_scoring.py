import sys
import math
from google.cloud import bigquery

# Placeholder for the BigQuery project ID. This replaces the implied Hive metastore configuration.
DEFAULT_BQ_PROJECT = "your-gcp-project-id"

def create_bigquery_client():
    """Initializes and returns a BigQuery client.
    Equivalent to SparkSession initialization, but for BigQuery.
    """
    return bigquery.Client(project=DEFAULT_BQ_PROJECT)

# The original haversine Python function for PySpark UDF.
# This logic is now directly embedded in BigQuery SQL queries as a CASE statement.
# The PySpark UDF registration (`haversine_udf = udf(haversine, DoubleType())`) is removed
# as BigQuery executes SQL, not Python UDFs directly from Python runtime.

def score_transactions_for_fraud(bq_client, execution_date):
    """
    Uses BigQuery SQL via Python client to score credit card transactions.
    Replaces PySpark DataFrame transformations with BigQuery SQL queries.
    """
    # Define fully qualified BigQuery table paths, replacing Spark's 'spark.table("db.table")'
    # assuming 'fin_core' and 'fin_mart' are BigQuery datasets within DEFAULT_BQ_PROJECT.
    full_cc_trx_table = f"`{DEFAULT_BQ_PROJECT}.fin_core.cc_transactions`"
    accounts_table = f"`{DEFAULT_BQ_PROJECT}.fin_core.dim_accounts`"
    merchants_table = f"`{DEFAULT_BQ_PROJECT}.fin_core.dim_merchants`"
    fraud_scores_table = f"`{DEFAULT_BQ_PROJECT}.fin_mart.fraud_scores_daily`"

    # The entire data processing pipeline is converted into a single BigQuery SQL query
    # using Common Table Expressions (CTEs) for clarity and efficiency.
    # This replaces sequential PySpark DataFrame API calls.
    main_query_sql = f"""
    WITH current_day_transactions AS (
        -- 1. Load Data & 2. Extract current day transactions
        -- Equivalent to `full_cc_trx.filter(col("trx_date") == execution_date)`
        SELECT
            trx_id,
            account_id,
            merchant_id,
            amount,
            trx_date,
            trx_time, -- Assuming trx_time column exists to form trx_timestamp
            TIMESTAMP(PARSE_DATE('%Y-%m-%d', CAST(trx_date AS STRING)), PARSE_TIME('%H:%M:%S', trx_time)) AS trx_timestamp
        FROM {full_cc_trx_table}
        WHERE trx_date = PARSE_DATE('%Y-%m-%d', @execution_date) -- Using parameterized query for date
    ),

    enriched_transactions AS (
        -- 3. Join location data and calculate distance Native
        -- Equivalent to .join().withColumn("distance_from_home_km", haversine_udf(...))
        SELECT
            t.* EXCEPT(account_id, merchant_id), -- BigQuery specific for selecting all but specified columns
            t.account_id,
            t.merchant_id,
            a.home_lat,
            a.home_lon,
            m.merchant_lat,
            m.merchant_lon,
            -- Haversine formula translated to BigQuery SQL functions
            CASE
                WHEN a.home_lat IS NULL OR a.home_lon IS NULL OR m.merchant_lat IS NULL OR m.merchant_lon IS NULL THEN -1.0
                ELSE
                    -- R = 6371.0 (Radius of earth in km)
                    2 * 6371.0 * ATAN2(
                        SQRT(
                            POW(SIN(RADIANS(m.merchant_lat - a.home_lat) / 2), 2) +
                            COS(RADIANS(a.home_lat)) * COS(RADIANS(m.merchant_lat)) *
                            POW(SIN(RADIANS(m.merchant_lon - a.home_lon) / 2), 2)
                        ),
                        SQRT(
                            1 - (
                                POW(SIN(RADIANS(m.merchant_lat - a.home_lat) / 2), 2) +
                                COS(RADIANS(a.home_lat)) * COS(RADIANS(m.merchant_lat)) *
                                POW(SIN(RADIANS(m.merchant_lon - a.home_lon) / 2), 2)
                            )
                        )
                    )
            END AS distance_from_home_km
        FROM current_day_transactions AS t
        INNER JOIN {accounts_table} AS a
            ON t.account_id = a.account_id
        LEFT JOIN {merchants_table} AS m
            ON t.merchant_id = m.merchant_id
    ),

    historical_transactions AS (
        -- 4. Pure DataFrame Historical Profiling Window
        -- Filter for the last 90 days of transactions (excluding execution date)
        -- Equivalent to `full_cc_trx.filter((col("trx_date") >= date_sub(...)) & (col("trx_date") <= date_sub(...)))`
        SELECT
            account_id,
            amount,
            trx_id -- Need trx_id for count
        FROM {full_cc_trx_table}
        WHERE
            trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 90 DAY)
            AND trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 1 DAY)
    ),

    historical_profile AS (
        -- Aggregate to build the historical profile
        -- Equivalent to `hist_trx.groupBy(...).agg(...)`
        SELECT
            account_id,
            AVG(amount) AS avg_trx_amount_90d,
            COALESCE(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d, -- BigQuery's STDDEV_SAMP can be NULL for single value, COALESCE for default 0.0
            COUNT(trx_id) / 90.0 AS avg_daily_trx_count
        FROM historical_transactions
        GROUP BY account_id
    ),

    features_joined AS (
        -- 5. Join current day transactions with their historical profiles
        -- Equivalent to `enriched_trx.alias("curr").join(hist_profile.alias("hist"), ...)`
        SELECT
            curr.*,
            hist.avg_trx_amount_90d,
            hist.stddev_trx_amount_90d,
            hist.avg_daily_trx_count
        FROM enriched_transactions AS curr
        LEFT JOIN historical_profile AS hist
            ON curr.account_id = hist.account_id
    ),

    calculated_window_features AS (
        -- 6. Apply Time-based Window Function (Last Hour Trx Count)
        -- Equivalent to `withColumn("trx_last_hour_cnt", count("curr.trx_id").over(time_window))`
        -- `unix_timestamp("curr.trx_timestamp")` -> `UNIX_SECONDS(curr.trx_timestamp)` for ordering in window.
        -- BigQuery's window function syntax `RANGE BETWEEN INTERVAL X SECOND PRECEDING AND CURRENT ROW` for time-based windows.
        SELECT
            *,
            COUNT(trx_id) OVER (
                PARTITION BY account_id
                ORDER BY trx_timestamp
                RANGE BETWEEN INTERVAL 3600 SECOND PRECEDING AND CURRENT ROW
            ) AS trx_last_hour_cnt,
            -- 7. Apply Complex Business Logic and Scoring Native DataFrame API (amount_z_score)
            -- Equivalent to `when(col("hist.stddev_trx_amount_90d") > 0, ...).otherwise(lit(0.0))`
            CASE
                WHEN stddev_trx_amount_90d IS NULL OR stddev_trx_amount_90d <= 0 THEN 0.0
                ELSE (amount - avg_trx_amount_90d) / stddev_trx_amount_90d
            END AS amount_z_score
        FROM features_joined
    ),

    final_scoring_calculations AS (
        -- 7. Apply Complex Business Logic and Scoring Native DataFrame API (risk scores and fraud score)
        SELECT
            *,
            -- Equivalent to `distance_risk_score = when((...) & (...), 30).otherwise(0)`
            CASE
                WHEN (distance_from_home_km > 500 AND distance_from_home_km != -1.0) THEN 30
                ELSE 0
            END AS distance_risk_score,
            -- Equivalent to `amount_risk_score = when(...).when(...).otherwise(0)`
            CASE
                WHEN amount_z_score > 3.0 THEN 40
                WHEN amount_z_score > 2.0 THEN 20
                ELSE 0
            END AS amount_risk_score,
            -- Equivalent to `velocity_risk_score = when(...).when(...).otherwise(0)`
            CASE
                WHEN trx_last_hour_cnt > 5 THEN 30
                WHEN trx_last_hour_cnt > 3 THEN 15
                ELSE 0
            END AS velocity_risk_score
        FROM calculated_window_features
    )
    -- 8. Select final columns
    -- Equivalent to `final_output = scored_df.select(...)`
    SELECT
        trx_id,
        account_id,
        amount,
        distance_from_home_km,
        amount_z_score,
        trx_last_hour_cnt,
        (distance_risk_score + amount_risk_score + velocity_risk_score) AS fraud_score,
        -- Equivalent to `is_fraud_alert = col("fraud_score") >= 60`
        (distance_risk_score + amount_risk_score + velocity_risk_score) >= 60 AS is_fraud_alert,
        PARSE_DATE('%Y-%m-%d', @execution_date) AS scoring_date
    FROM final_scoring_calculations
    """

    # Construct the final INSERT INTO statement for BigQuery
    # This replaces `.write.mode("append").insertInto(...)`
    insert_statement = f"""
    INSERT INTO {fraud_scores_table} (
        trx_id, account_id, amount, distance_from_home_km, amount_z_score,
        trx_last_hour_cnt, fraud_score, is_fraud_alert, scoring_date
    )
    {main_query_sql}
    """

    # Configure and execute the BigQuery query using query parameters for safety
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("execution_date", "STRING", execution_date)
        ]
    )

    # Start the query job
    query_job = bq_client.query(insert_statement, job_config=job_config)

    # Wait for the job to complete and print confirmation/errors
    try:
        query_job.result()  # Waits for the job to complete
        print(f"BigQuery Fraud scoring completed for {execution_date}")
    except Exception as e:
        print(f"Error during BigQuery job for {execution_date}: {e}")
        if query_job.errors: # Print BigQuery specific errors if available
            for error in query_job.errors:
                print(f"BigQuery Error Detail: {error.get('message', 'N/A')}")
        raise # Re-raise to ensure script failure on BQ error

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fin_credit_card_fraud_scoring.py <YYYY-MM-DD>")
        sys.exit(1)

    exec_date = sys.argv[1]
    # Replaces `sp = create_spark_session()` with BigQuery client creation
    bq_client = create_bigquery_client()

    score_transactions_for_fraud(bq_client, exec_date)
    # No explicit `sp.stop()` needed for BigQuery client as it manages connections statelessy.
