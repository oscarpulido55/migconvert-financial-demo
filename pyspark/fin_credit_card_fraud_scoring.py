import sys
import math
# Removed PySpark specific imports.
from google.cloud import bigquery # New import for BigQuery client library.
# The original 'pyspark.sql.types' (DoubleType, IntegerType, StringType) are implicitly handled by BigQuery SQL types.
# The original 'pyspark.sql.window' is converted to BigQuery SQL window function syntax.

# The create_spark_session function is removed as SparkSession is not used.
# BigQuery client initialization handles database connection.

# The Python haversine function is retained but will not be directly used in the BigQuery query.
# BigQuery's native geospatial functions (ST_GEOGPOINT, ST_DISTANCE) are used for efficiency.
def haversine(lat1, lon1, lat2, lon2):
    """Calculates the great circle distance between two points on the earth."""
    # BigQuery's ST_DISTANCE handles NULL coordinates gracefully by returning NULL,
    # which is then COALESCE'd to -1.0 to match original UDF behavior.
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return -1.0
    R = 6371.0 # Radius of earth in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# Spark UDF registration (haversine_udf) is not applicable; BigQuery SQL functions replace its role.

def score_transactions_for_fraud(client: bigquery.Client, execution_date: str):
    """
    Converts the PySpark DataFrame APIs logic into a BigQuery SQL query,
    executed via the BigQuery Python client. This allows BigQuery to perform
    the distributed processing.
    """

    # BigQuery table identifiers are constructed. Ensure project ID is set for the client.
    project_id = client.project
    full_cc_trx_table_id = f"{project_id}.fin_core.cc_transactions"
    accounts_table_id = f"{project_id}.fin_core.dim_accounts"
    merchants_table_id = f"{project_id}.fin_core.dim_merchants"
    output_table_id = f"{project_id}.fin_mart.fraud_scores_daily"

    # BigQuery parameters for the query to prevent SQL injection and improve readability.
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("execution_date", "STRING", execution_date),
        ]
    )

    # The entire data processing logic is translated into a single BigQuery SQL query
    # using Common Table Expressions (CTEs) for modularity, mirroring PySpark DataFrame steps.
    sql_query = f"""
    WITH
    current_day_transactions AS (
        -- Corresponds to PySpark's full_cc_trx.filter(col("trx_date") == execution_date)
        -- and initial joins.
        SELECT
            t.trx_id,
            t.account_id,
            t.amount,
            t.trx_timestamp,
            t.trx_date,
            a.home_lat,
            a.home_lon,
            m.merchant_lat,
            m.merchant_lon
        FROM
            `{full_cc_trx_table_id}` AS t
        INNER JOIN
            `{accounts_table_id}` AS a
            ON t.account_id = a.account_id
        LEFT JOIN
            `{merchants_table_id}` AS m
            ON t.merchant_id = m.merchant_id
        WHERE
            t.trx_date = PARSE_DATE('%Y-%m-%d', @execution_date)
    ),
    enriched_transactions AS (
        -- Corresponds to PySpark's withColumn("distance_from_home_km", haversine_udf(...))
        -- BigQuery GIS functions (ST_GEOGPOINT, ST_DISTANCE) are used, which expect (longitude, latitude).
        -- ST_DISTANCE returns meters, divided by 1000 for km. COALESCE handles NULL coordinates returning -1.0.
        SELECT
            *,
            COALESCE(
                ST_DISTANCE(
                    ST_GEOGPOINT(home_lon, home_lat),
                    ST_GEOGPOINT(merchant_lon, merchant_lat)
                ) / 1000,
                -1.0
            ) AS distance_from_home_km
        FROM
            current_day_transactions
    ),
    historical_transactions AS (
        -- Corresponds to PySpark's hist_trx filter.
        -- DATE_SUB is used for date arithmetic, PARSE_DATE converts string to DATE type.
        SELECT
            account_id,
            amount,
            trx_id
        FROM
            `{full_cc_trx_table_id}`
        WHERE
            trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 90 DAY)
            AND trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 1 DAY)
    ),
    historical_profile AS (
        -- Corresponds to PySpark's groupBy().agg() for historical profiling.
        -- COALESCE(STDDEV_SAMP(...), 0.0) handles cases where standard deviation is undefined (e.g., single observation).
        SELECT
            account_id,
            AVG(amount) AS avg_trx_amount_90d,
            COALESCE(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d,
            COUNT(trx_id) / 90.0 AS avg_daily_trx_count
        FROM
            historical_transactions
        GROUP BY
            account_id
    ),
    features AS (
        -- Corresponds to PySpark's join of current transactions with historical profiles.
        SELECT
            curr.*,
            hist.avg_trx_amount_90d,
            hist.stddev_trx_amount_90d,
            hist.avg_daily_trx_count
        FROM
            enriched_transactions AS curr
        LEFT JOIN
            historical_profile AS hist
            ON curr.account_id = hist.account_id
    ),
    scored_data AS (
        -- Corresponds to PySpark's withColumn for window functions and complex business logic.
        -- UNIX_SECONDS converts TIMESTAMP to epoch seconds, used for RANGE BETWEEN window frame.
        -- CASE WHEN ... THEN ... ELSE ... END replaces PySpark's when().otherwise().
        SELECT
            *,
            COUNT(trx_id) OVER (
                PARTITION BY account_id
                ORDER BY UNIX_SECONDS(trx_timestamp)
                RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
            ) AS trx_last_hour_cnt,
            CASE
                WHEN stddev_trx_amount_90d IS NOT NULL AND stddev_trx_amount_90d > 0 THEN
                    (amount - avg_trx_amount_90d) / stddev_trx_amount_90d
                ELSE
                    0.0
            END AS amount_z_score,
            CASE
                WHEN distance_from_home_km > 500 AND distance_from_home_km != -1.0 THEN 30
                ELSE 0
            END AS distance_risk_score,
            CASE
                WHEN (amount_z_score IS NOT NULL AND amount_z_score > 3.0) THEN 40
                WHEN (amount_z_score IS NOT NULL AND amount_z_score > 2.0) THEN 20
                ELSE 0
            END AS amount_risk_score,
            CASE
                WHEN trx_last_hour_cnt > 5 THEN 30
                WHEN trx_last_hour_cnt > 3 THEN 15
                ELSE 0
            END AS velocity_risk_score
        FROM
            features
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
    FROM
        scored_data
    """

    # Executes the generated BigQuery SQL query.
    # The 'insertInto' PySpark method is replaced by an 'INSERT INTO' BigQuery SQL statement.
    final_sql_query = f"""
    INSERT INTO `{output_table_id}` (
        trx_id, account_id, amount, distance_from_home_km, amount_z_score,
        trx_last_hour_cnt, fraud_score, is_fraud_alert, scoring_date
    )
    {sql_query}
    """

    query_job = client.query(final_sql_query, job_config=job_config)
    query_job.result() # Waits for the BigQuery job to complete. This is blocking.

    # Print statement is part of the original script.
    print(f"Pure BigQuery Fraud scoring completed for {execution_date}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fin_credit_card_fraud_scoring_bq.py <YYYY-MM-DD>")
        sys.exit(1)

    exec_date = sys.argv[1]
    # Replaces the SparkSession creation. The BigQuery client manages connections.
    bq_client = bigquery.Client() # Initializes BigQuery client using default credentials.

    score_transactions_for_fraud(bq_client, exec_date)
    # No explicit stop method for BigQuery client is needed as it's stateless for queries.
    # The original sp.stop() is therefore removed.
