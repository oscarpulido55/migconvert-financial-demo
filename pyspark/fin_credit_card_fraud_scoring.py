import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID; // Used for generating unique BigQuery job IDs

// This class represents the conversion of the Python/PySpark script
// to Java, executing the equivalent logic as BigQuery SQL queries.
public class FinancialCreditCardFraudScoring {

    /**
     * Initializes and returns a BigQuery service client.
     * Replaces the PySpark create_spark_session function.
     * It relies on Google Cloud's default authentication mechanisms (e.g.,
     * GOOGLE_APPLICATION_CREDENTIALS environment variable or default service account).
     */
    private static BigQuery getBigQueryService() {
        return BigQueryOptions.getDefaultInstance().getService();
    }

    /**
     * Executes a BigQuery SQL query string.
     *
     * @param bigquery The initialized BigQuery client.
     * @param querySql The SQL query to execute.
     * @param jobName  A descriptive base name for the BigQuery job.
     * @throws InterruptedException If the BigQuery job is interrupted.
     * This method directly executes the DML/DDL query. No result set is expected
     * for INSERT operations like the final step.
     */
    private static void executeBigQueryQuery(BigQuery bigquery, String querySql, String jobName) throws InterruptedException {
        // Build the query configuration.
        // We set useLegacySql(false) to ensure standard SQL is used.
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(querySql)
                .setUseLegacySql(false)
                .build();

        // Generate a unique job ID to ensure idempotency and avoid conflicts.
        String fullJobId = jobName + "_" + UUID.randomUUID().toString();

        System.out.println("Executing BigQuery job: " + fullJobId);
        // In a real application, you might log the full query for debugging,
        // but it can be very long. For now, we'll keep it concise.
        // System.out.println("Query:\n" + querySql);

        // Execute the query. BigQuery's query method returns a Job object,
        // from which we can get the TableResult if it's a SELECT query.
        // For DML like INSERT, TableResult might be empty but indicates success.
        bigquery.query(queryConfig);
    }

    /**
     * Scores credit card transactions for fraud using BigQuery SQL,
     * converting PySpark DataFrame API logic to BigQuery SQL constructs.
     *
     * @param bigquery      The BigQuery client.
     * @param executionDate The date for which to score transactions (YYYY-MM-DD).
     * @throws InterruptedException If the BigQuery job execution is interrupted.
     */
    public static void scoreTransactionsForFraud(BigQuery bigquery, String executionDate) throws InterruptedException {
        // In BigQuery, PySpark DataFrame operations are typically translated
        // into a single, comprehensive SQL query using Common Table Expressions (CTEs).

        // Custom UDF for Haversine distance, matching the Python UDF's logic
        // and handling the -1.0 return for null inputs. This is defined as a
        // TEMPORARY FUNCTION within the SQL query, available only for the session.
        // The radius of Earth (R) is 6371.0 km, as in the Python code.
        String haversineUDF =
            "CREATE TEMPORARY FUNCTION haversine(lat1 FLOAT64, lon1 FLOAT64, lat2 FLOAT64, lon2 FLOAT64) RETURNS FLOAT64 AS (\n" +
            "  IF(lat1 IS NULL OR lon1 IS NULL OR lat2 IS NULL OR lon2 IS NULL, -1.0, (\n" +
            "    (2 * 6371.0) * ATAN2(SQRT(\n" +
            "      POW(SIN(RADIANS(lat2 - lat1)/2), 2) +\n" +
            "      COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *\n" +
            "      POW(SIN(RADIANS(lon2 - lon1)/2), 2)\n" +
            "    ), SQRT(1 - (\n" +
            "      POW(SIN(RADIANS(lat2 - lat1)/2), 2) +\n" +
            "      COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *\n" +
            "      POW(SIN(RADIANS(lon2 - lon1)/2), 2)\n" +
            "    )))\n" +
            "  ))\n" +
            ");\n";

        // Construct the main BigQuery SQL query by chaining operations via CTEs.
        // This converts the entire PySpark DataFrame transformation flow.
        String fraudScoringQuery =
            // 1. Define the Haversine UDF at the beginning of the query execution.
            haversineUDF +

            // 1. Load Data (implicit in CTEs) and 2. Extract current day transactions.
            // Replaced spark.table("fin_core.cc_transactions").filter(...)
            // with a SELECT statement targeting the BigQuery table.
            // PARSE_DATE('%Y-%m-%d', ...) is used to convert string to DATE type.
            "WITH current_day_transactions AS (\n" +
            "  SELECT *\n" +
            "  FROM `fin_core.cc_transactions`\n" + // BigQuery uses backticks for table identifiers.
            "  WHERE trx_date = PARSE_DATE('%Y-%m-%d', '" + executionDate + "')\n" +
            "),\n" +

            // Define dimension tables as CTEs for clear source definition.
            // These correspond to spark.table("fin_core.dim_accounts") and spark.table("fin_core.dim_merchants").
            "accounts_dim AS (\n" +
            "  SELECT account_id, home_lat, home_lon\n" +
            "  FROM `fin_core.dim_accounts`\n" +
            "),\n" +
            "merchants_dim AS (\n" +
            "  SELECT merchant_id, merchant_lat, merchant_lon\n" +
            "  FROM `fin_core.dim_merchants`\n" +
            "),\n" +

            // 3. Join location data and calculate distance.
            // Corresponds to .join() and .withColumn(haversine_udf(...)).
            "enriched_transactions AS (\n" +
            "  SELECT\n" +
            "    t.*,\n" +
            "    haversine(a.home_lat, a.home_lon, m.merchant_lat, m.merchant_lon) AS distance_from_home_km\n" +
            "  FROM current_day_transactions AS t\n" +
            "  INNER JOIN accounts_dim AS a ON t.account_id = a.account_id\n" +
            "  LEFT JOIN merchants_dim AS m ON t.merchant_id = m.merchant_id\n" +
            "),\n" +

            // 4. Historical Profiling (90-day window).
            // Replaced spark.table().filter(...).groupBy(...).agg(...) with BigQuery SQL.
            // DATE_SUB(PARSE_DATE, INTERVAL N DAY) is BigQuery's equivalent to date_sub.
            // COALESCE(STDDEV_SAMP, 0.0) handles cases where standard deviation might be NULL for single-row groups.
            "historical_transactions AS (\n" +
            "  SELECT *\n" +
            "  FROM `fin_core.cc_transactions`\n" +
            "  WHERE trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', '" + executionDate + "'), INTERVAL 90 DAY)\n" +
            "    AND trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', '" + executionDate + "'), INTERVAL 1 DAY)\n" +
            "),\n" +
            "historical_profile AS (\n" +
            "  SELECT\n" +
            "    account_id,\n" +
            "    AVG(amount) AS avg_trx_amount_90d,\n" +
            "    COALESCE(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d,\n" +
            "    COUNT(trx_id) / 90.0 AS avg_daily_trx_count\n" +
            "  FROM historical_transactions\n" +
            "  GROUP BY account_id\n" +
            "),\n" +

            // 5. Join current day transactions with their historical profiles.
            // Corresponds to the second .join() operation.
            "features_joined AS (\n" +
            "  SELECT\n" +
            "    curr.*,\n" +
            "    hist.avg_trx_amount_90d,\n" +
            "    hist.stddev_trx_amount_90d,\n" +
            "    hist.avg_daily_trx_count\n" +
            "  FROM enriched_transactions AS curr\n" +
            "  LEFT JOIN historical_profile AS hist ON curr.account_id = hist.account_id\n" +
            "),\n" +

            // 6. Apply Time-based Window Function (Last Hour Trx Count) and 7. Complex Business Logic and Scoring.
            // PySpark's Window.partitionBy().orderBy(unix_timestamp()).rangeBetween(-3600, 0)
            // is converted to BigQuery SQL's window function using UNIX_SECONDS for ordering.
            // RANGE BETWEEN 3600 * (-1) PRECEDING AND 0 PRECEDING specifies the 1-hour (3600 seconds)
            // window based on the timestamp.
            // PySpark's when().otherwise() is translated to BigQuery's CASE WHEN ... THEN ... ELSE ... END.
            "scored_intermediate AS (\n" +
            "  SELECT\n" +
            "    *,\n" +
            "    COUNT(trx_id) OVER (\n" +
            "      PARTITION BY account_id\n" +
            "      ORDER BY UNIX_SECONDS(trx_timestamp)\n" +
            "      RANGE BETWEEN 3600 * (-1) PRECEDING AND 0 PRECEDING\n" + // 3600 seconds = 1 hour
            "    ) AS trx_last_hour_cnt,\n" +
            "    CASE\n" +
            "      WHEN stddev_trx_amount_90d > 0\n" +
            "      THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d\n" +
            "      ELSE 0.0\n" +
            "    END AS amount_z_score\n" +
            "  FROM features_joined\n" +
            "),\n" +
            "final_scoring AS (\n" +
            "  SELECT\n" +
            "    *,\n" +
            "    CASE\n" +
            "      WHEN distance_from_home_km > 500 AND distance_from_home_km != -1.0 THEN 30\n" +
            "      ELSE 0\n" +
            "    END AS distance_risk_score,\n" +
            "    CASE\n" +
            "      WHEN amount_z_score > 3.0 THEN 40\n" +
            "      WHEN amount_z_score > 2.0 THEN 20\n" +
            "      ELSE 0\n" +
            "    END AS amount_risk_score,\n" +
            "    CASE\n" +
            "      WHEN trx_last_hour_cnt > 5 THEN 30\n" +
            "      WHEN trx_last_hour_cnt > 3 THEN 15\n" +
            "      ELSE 0\n" +
            "    END AS velocity_risk_score\n" +
            "  FROM scored_intermediate\n" +
            ")\n" +

            // 8. Select final columns and write to target.
            // Corresponds to final_output.select(...) and .write.mode("append").insertInto(...).
            // BigQuery uses INSERT INTO ... SELECT for appending data.
            "INSERT INTO `fin_mart.fraud_scores_daily` (\n" +
            "  trx_id, account_id, amount, distance_from_home_km, amount_z_score,\n" +
            "  trx_last_hour_cnt, fraud_score, is_fraud_alert, scoring_date\n" +
            ")\n" +
            "SELECT\n" +
            "  trx_id,\n" +
            "  account_id,\n" +
            "  amount,\n" +
            "  distance_from_home_km,\n" +
            "  amount_z_score,\n" +
            "  trx_last_hour_cnt,\n" +
            "  (distance_risk_score + amount_risk_score + velocity_risk_score) AS fraud_score,\n" +
            "  (distance_risk_score + amount_risk_score + velocity_risk_score) >= 60 AS is_fraud_alert,\n" +
            "  PARSE_DATE('%Y-%m-%d', '" + executionDate + "') AS scoring_date\n" +
            "FROM final_scoring;";

        try {
            // Execute the constructed BigQuery SQL query.
            executeBigQueryQuery(bigquery, fraudScoringQuery, "fraud_scoring_job");
            System.out.println("BigQuery Fraud scoring completed for " + executionDate);
        } catch (InterruptedException e) {
            System.err.println("Error executing BigQuery fraud scoring job: " + e.getMessage());
            Thread.currentThread().interrupt(); // Restore the interrupted status for the calling thread.
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // Python's `if __name__ == "__main__":` block is converted to Java's `public static void main`.
        if (args.length < 1) {
            System.out.println("Usage: java FinancialCreditCardFraudScoring <YYYY-MM-DD>");
            System.exit(1); // Exit with an error code.
        }

        String execDate = args[0]; // Get execution date from command line arguments.
        BigQuery bigqueryService = getBigQueryService(); // Initialize BigQuery client.

        scoreTransactionsForFraud(bigqueryService, execDate); // Run the fraud scoring logic.
        // BigQuery client instances usually manage their resources automatically and don't
        // require an explicit 'stop()' call like SparkSession.
    }
}
