import sys
import math
# Replace PySpark imports with BigQuery client library
from google.cloud import bigquery
from google.cloud.exceptions import NotFound # For checking UDF existence

# The PySpark SparkSession creation is replaced by a BigQuery client.
# This function initializes and returns a BigQuery client.
def create_bigquery_client():
    """Initializes and returns a BigQuery client."""
    # BigQuery client automatically handles authentication (e.g., via GOOGLE_APPLICATION_CREDENTIALS)
    return bigquery.Client()

# The original haversine Python function.
# In BigQuery, this logic will be implemented as a SQL UDF or a JavaScript UDF directly in BigQuery.
# This Python function now serves as the source logic for the BigQuery UDF definition below.
# It is commented out as it's not executed directly by Python for BigQuery operations,
# but its logic is transferred.
# def haversine(lat1, lon1, lat2, lon2):
#     """Calculates the great circle distance between two points on the earth."""
#     if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
#         return -1.0
#     R = 6371.0 # Radius of earth in km
#     phi1, phi2 = math.radians(lat1), math.radians(lat2)
#     dphi = math.radians(lat2 - lat1)
#     dlambda = math.radians(lon2 - lon1)
#     a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
#     return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# Conversion of haversine_udf = udf(haversine, DoubleType())
# This block dynamically creates or replaces a BigQuery JavaScript UDF.
# This UDF replaces the PySpark Python UDF for calculating Haversine distance within BigQuery SQL.
# Ensure 'fin_core' dataset exists in your BigQuery project.
def create_haversine_udf(client, project_id, dataset_id):
    udf_fqn = f"`{project_id}.{dataset_id}.haversine`" # Fully Qualified Name for the UDF
    try:
        # Check if the UDF already exists (BigQuery routines API uses ROUTINES.udf_name convention)
        client.get_routine(f"{project_id}.{dataset_id}.haversine")
        print(f"BigQuery UDF {udf_fqn} already exists. Skipping creation.")
    except NotFound:
        print(f"Creating BigQuery UDF {udf_fqn}...")
        create_udf_sql = f"""
        CREATE OR REPLACE FUNCTION {udf_fqn}(
            lat1 FLOAT64, lon1 FLOAT64, lat2 FLOAT64, lon2 FLOAT64)
        RETURNS FLOAT64
        LANGUAGE js AS '''
            if (lat1 === null || lon1 === null || lat2 === null || lon2 === null) {
                return -1.0;
            }
            const R = 6371.0; // Radius of earth in km
            const phi1 = lat1 * Math.PI / 180;
            const phi2 = lat2 * Math.PI / 180;
            const dphi = (lat2 - lat1) * Math.PI / 180;
            const dlambda = (lon2 - lon1) * Math.PI / 180;
            const a = Math.sin(dphi / 2) * Math.sin(dphi / 2) +
                      Math.cos(phi1) * Math.cos(phi2) *
                      Math.sin(dlambda / 2) * Math.sin(dlambda / 2);
            return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        ''';
        """
        client.query(create_udf_sql).result()
        print(f"BigQuery UDF {udf_fqn} created.")

# The function score_transactions_for_fraud is converted to construct and execute BigQuery SQL.
# The 'spark' argument is replaced by 'bigquery_client'.
def score_transactions_for_fraud(bigquery_client, execution_date):
    """
    Constructs and executes a BigQuery SQL query to score credit card transactions.
    Replaces PySpark DataFrame APIs with BigQuery SQL CTEs and BigQuery functions.
    """
    # Assuming the BigQuery project ID is configured in the client or environment
    project_id = bigquery_client.project
    core_dataset_id = "fin_core" # Replace with your BigQuery dataset for core data
    mart_dataset_id = "fin_mart" # Replace with your BigQuery dataset for mart data

    # Ensure the Haversine UDF is created in BigQuery
    create_haversine_udf(bigquery_client, project_id, core_dataset_id)

    # SQL query construction to replicate the PySpark DataFrame logic
    # Each step of the original PySpark script is translated into a CTE (Common Table Expression).
    # This structure mirrors the sequential DataFrame transformations.
    sql_query = f"""
    -- 1. Load Data (implicitly done by referencing tables in CTEs)
    WITH full_cc_trx AS (
        SELECT * FROM `{project_id}.{core_dataset_id}.cc_transactions`
    ),
    accounts AS (
        SELECT * FROM `{project_id}.{core_dataset_id}.dim_accounts`
    ),
    merchants AS (
        SELECT * FROM `{project_id}.{core_dataset_id}.dim_merchants`
    ),

    -- 2. Extract current day transactions
    -- Replaces: cc_trx = full_cc_trx.filter(col("trx_date") == execution_date)
    cc_trx_current_day AS (
        SELECT *
        FROM full_cc_trx
        WHERE trx_date = PARSE_DATE('%Y-%m-%d', @execution_date)
    ),

    -- 3. Join location data and calculate distance Native
    -- Replaces: .join().withColumn("distance_from_home_km", haversine_udf(...))
    enriched_trx AS (
        SELECT
            t.*, -- Select all columns from transactions
            a.home_lat,
            a.home_lon,
            m.merchant_lat,
            m.merchant_lon,
            -- Call the BigQuery UDF 'fin_core.haversine' instead of PySpark's haversine_udf
            `{project_id}`.{core_dataset_id}.haversine(a.home_lat, a.home_lon, m.merchant_lat, m.merchant_lon) AS distance_from_home_km
        FROM cc_trx_current_day AS t
        INNER JOIN accounts AS a ON t.account_id = a.account_id
        LEFT JOIN merchants AS m ON t.merchant_id = m.merchant_id
    ),

    -- 4. Pure DataFrame Historical Profiling Window
    -- Filter for the last 90 days of transactions (excluding execution date)
    -- Replaces: hist_trx = full_cc_trx.filter((col("trx_date") >= date_sub(...)) & (col("trx_date") <= date_sub(...)))
    hist_trx AS (
        SELECT *
        FROM full_cc_trx
        WHERE
            trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 90 DAY) AND
            trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', @execution_date), INTERVAL 1 DAY)
    ),
    -- Aggregate to build the historical profile
    -- Replaces: hist_profile = hist_trx.groupBy("account_id").agg(...).fillna(...)
    hist_profile AS (
        SELECT
            account_id,
            AVG(amount) AS avg_trx_amount_90d,
            -- COALESCE handles cases where STDDEV_SAMP is NULL (e.g., single transaction), equivalent to fillna(0.0)
            COALESCE(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d,
            COUNT(trx_id) / 90.0 AS avg_daily_trx_count
        FROM hist_trx
        GROUP BY account_id
    ),

    -- 5. Join current day transactions with their historical profiles
    -- Replaces: df_features = enriched_trx.alias("curr").join(hist_profile.alias("hist"), ...)
    df_features AS (
        SELECT
            curr.*, -- Select all current transaction features
            hist.avg_trx_amount_90d,
            hist.stddev_trx_amount_90d,
            hist.avg_daily_trx_count
        FROM enriched_trx AS curr
        LEFT JOIN hist_profile AS hist ON curr.account_id = hist.account_id
    ),

    -- 6. Apply Time-based Window Function (Last Hour Trx Count)
    -- Replaces: .withColumn("trx_last_hour_cnt", count("curr.trx_id").over(time_window))
    trx_with_velocity AS (
        SELECT
            *, -- Select all existing columns from df_features
            COUNT(trx_id) OVER (
                PARTITION BY account_id
                ORDER BY UNIX_SECONDS(trx_timestamp) -- UNIX_SECONDS for timestamp ordering and RANGE
                RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW -- Equivalent to PySpark rangeBetween(-3600, 0)
            ) AS trx_last_hour_cnt
        FROM df_features
    ),

    -- 7. Apply Complex Business Logic and Scoring Native DataFrame API
    -- Replaces multiple .withColumn() operations
    intermediate_scores AS (
        SELECT
            *, -- Select all columns from trx_with_velocity
            -- Calculate amount_z_score using CASE WHEN, similar to PySpark's when().otherwise()
            CASE
                WHEN stddev_trx_amount_90d > 0 THEN
                    (amount - avg_trx_amount_90d) / stddev_trx_amount_90d
                ELSE 0.0
            END AS amount_z_score,
            -- Calculate distance_risk_score
            CASE
                WHEN (distance_from_home_km > 500 AND distance_from_home_km != -1.0) THEN 30
                ELSE 0
            END AS distance_risk_score,
            -- Calculate amount_risk_score; amount_z_score needs to be recalculated or from a prior CTE
            CASE
                WHEN (CASE WHEN stddev_trx_amount_90d > 0 THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d ELSE 0.0 END) > 3.0 THEN 40
                WHEN (CASE WHEN stddev_trx_amount_90d > 0 THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d ELSE 0.0 END) > 2.0 THEN 20
                ELSE 0
            END AS amount_risk_score,
            -- Calculate velocity_risk_score
            CASE
                WHEN trx_last_hour_cnt > 5 THEN 30
                WHEN trx_last_hour_cnt > 3 THEN 15
                ELSE 0
            END AS velocity_risk_score
        FROM trx_with_velocity
    ),

    -- Final calculation of fraud_score and is_fraud_alert
    scored_df AS (
        SELECT
            *, -- Select all columns from intermediate_scores
            -- fraud_score
            (distance_risk_score + amount_risk_score + velocity_risk_score) AS fraud_score,
            -- is_fraud_alert (boolean expression)
            (distance_risk_score + amount_risk_score + velocity_risk_score) >= 60 AS is_fraud_alert
        FROM intermediate_scores
    )

    -- 8. Select final columns and write to target
    -- Replaces: final_output.write.mode("append").insertInto("fin_mart.fraud_scores_daily")
    INSERT INTO `{project_id}`.{mart_dataset_id}.fraud_scores_daily (
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
        fraud_score,
        is_fraud_alert,
        PARSE_DATE('%Y-%m-%d', @execution_date) AS scoring_date
    FROM scored_df;
    """

    # Define query parameters, equivalent to passing variables to PySpark filters/expressions.
    query_params = [
        bigquery.ScalarQueryParameter("execution_date", "STRING", execution_date)
    ]

    # Execute the BigQuery SQL query.
    # The .result() method blocks until the query completes, mimicking Spark's blocking actions.
    job = bigquery_client.query(sql_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
    job.result() # Wait for the job to complete
    
    print(f"BigQuery Fraud scoring completed for {execution_date}")

# Entry point of the script, handling command-line arguments and orchestrating execution.
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fin_credit_card_fraud_scoring_bq.py <YYYY-MM-DD>")
        sys.exit(1)
        
    exec_date = sys.argv[1]
    # Replaces create_spark_session() with BigQuery client creation.
    bq_client = create_bigquery_client() 
    
    score_transactions_for_fraud(bq_client, exec_date)
    # No explicit stop method is typically needed for google.cloud.bigquery.Client().
    # sp.stop() # Original PySpark line removed.