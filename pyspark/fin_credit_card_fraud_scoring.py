To translate the given Python Spark script into a production-ready Java Spark application, we will follow the instructions to replace all standard print statements with `java.util.logging.Logger` and convert the Spark DataFrame operations from PySpark to Java Spark.

We will use the `java.util.logging.Logger` as requested for all logging and exception reporting.

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.api.java.UDF5; // For UDFs with 5 arguments

import java.io.Serializable;
import java.util.logging.Level; // For specifying log levels like SEVERE, INFO, WARNING
import java.util.logging.Logger; // The requested logger
import java.lang.Math; // For math operations like toRadians, sin, cos, pow, sqrt, atan2

import static org.apache.spark.sql.functions.*; // Import all static methods from functions for brevity

/**
 * Java translation of fin_credit_card_fraud_scoring.py.
 * This class performs fraud scoring on credit card transactions using Spark DataFrame API.
 * It replaces standard output prints with java.util.logging.Logger for better monitoring and error handling.
 */
public class FinCreditCardFraudScoring implements Serializable {

    // Initialize the logger for this class
    private static final Logger LOGGER = Logger.getLogger(FinCreditCardFraudScoring.class.getName());

    // Define a constant for the UDF name for consistency
    private static final String HAVERSINE_UDF_NAME = "haversine_distance";

    /**
     * Helper method to calculate the great circle distance between two points on the earth (Haversine formula).
     * @param lat1 Latitude of point 1
     * @param lon1 Longitude of point 1
     * @param lat2 Latitude of point 2
     * @param lon2 Longitude of point 2
     * @return Distance in kilometers, or -1.0 if any coordinate is null.
     */
    private static Double calculateHaversine(Double lat1, Double lon1, Double lat2, Double lon2) {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
            return -1.0;
        }
        double R = 6371.0; // Radius of earth in km
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dphi = Math.toRadians(lat2 - lat1);
        double dlambda = Math.toRadians(lon2 - lon1);

        double a = Math.pow(Math.sin(dphi / 2), 2) +
                   Math.cos(phi1) * Math.cos(phi2) *
                   Math.pow(Math.sin(dlambda / 2), 2);
        return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // Define the Spark UDF wrapper for the haversine calculation
    private static UDF5<Double, Double, Double, Double, Double> haversineSparkUDF =
        (lat1, lon1, lat2, lon2) -> calculateHaversine(lat1, lon1, lat2, lon2);


    /**
     * Initializes and returns a Spark session with Hive Metastore support.
     *
     * @return SparkSession instance.
     */
    private static SparkSession createSparkSession() {
        LOGGER.info("Creating SparkSession...");
        SparkSession spark = SparkSession.builder()
                .appName("Financial_Credit_Card_Fraud_Scoring")
                .enableHiveSupport()
                .getOrCreate();
        LOGGER.info("SparkSession created successfully.");
        return spark;
    }

    /**
     * Uses pure Spark DataFrame APIs to score credit card transactions for fraud.
     * Replaces embedded SQL with continuous DataFrame transformations.
     *
     * @param spark         The SparkSession.
     * @param executionDate The date for which to process transactions (YYYY-MM-DD).
     */
    public static void scoreTransactionsForFraud(SparkSession spark, String executionDate) {
        LOGGER.info("Starting fraud scoring process for execution date: " + executionDate);

        // Register the Haversine UDF with the SparkSession
        spark.udf().register(HAVERSINE_UDF_NAME, haversineSparkUDF, DataTypes.DoubleType);
        LOGGER.info("Haversine UDF '" + HAVERSINE_UDF_NAME + "' registered.");

        // 1. Load Data
        LOGGER.info("Loading data from Hive tables: fin_core.cc_transactions, fin_core.dim_accounts, fin_core.dim_merchants.");
        Dataset<Row> fullCcTrx = spark.table("fin_core.cc_transactions");
        Dataset<Row> accounts = spark.table("fin_core.dim_accounts");
        Dataset<Row> merchants = spark.table("fin_core.dim_merchants");
        LOGGER.info("Successfully loaded initial datasets.");

        // 2. Extract current day transactions
        Dataset<Row> ccTrx = fullCcTrx.filter(col("trx_date").equalTo(lit(executionDate)));
        LOGGER.info("Filtered transactions for current execution date: " + executionDate + ".");
        if (ccTrx.isEmpty()) {
            LOGGER.warning("No transactions found for " + executionDate + ". Skipping fraud scoring for this date.");
            return; // Exit early if no transactions for the day
        }

        // 3. Join location data and calculate distance using the registered UDF
        LOGGER.info("Joining transaction data with account and merchant dimensions and calculating distance from home.");
        Dataset<Row> enrichedTrx = ccTrx.alias("t")
                .join(accounts.alias("a"), col("t.account_id").equalTo(col("a.account_id")), "inner")
                .join(merchants.alias("m"), col("t.merchant_id").equalTo(col("m.merchant_id")), "left")
                .withColumn(
                        "distance_from_home_km",
                        callUDF(HAVERSINE_UDF_NAME,
                                col("a.home_lat"), col("a.home_lon"), col("m.merchant_lat"), col("m.merchant_lon"))
                );
        LOGGER.info("Enrichment and distance calculation complete.");

        // 4. Historical Profiling Window - Filter for the last 90 days (excluding execution date)
        LOGGER.info("Filtering historical transactions for the last 90 days to build account profiles.");
        Dataset<Row> histTrx = fullCcTrx.filter(
                col("trx_date").geq(date_sub(lit(executionDate), 90))
                        .and(col("trx_date").leq(date_sub(lit(executionDate), 1)))
        );

        // Aggregate to build the historical profile for each account
        Dataset<Row> histProfile = histTrx.groupBy("account_id").agg(
                avg(col("amount")).alias("avg_trx_amount_90d"),
                stddev_samp(col("amount")).alias("stddev_trx_amount_90d"),
                (count(col("trx_id")).divide(lit(90.0))).alias("avg_daily_trx_count")
        )
        // Fill null stddev (e.g., for accounts with only one transaction) with 0.0 to avoid NaNs later
        .na().fill(0.0, new String[]{"stddev_trx_amount_90d"});
        LOGGER.info("Historical profiles for accounts generated.");

        // 5. Join current day transactions with their historical profiles
        LOGGER.info("Joining current day transactions with historical account profiles.");
        Dataset<Row> dfFeatures = enrichedTrx.alias("curr")
                .join(histProfile.alias("hist"), col("curr.account_id").equalTo(col("hist.account_id")), "left");
        LOGGER.info("Feature DataFrame created by joining current and historical data.");

        // 6. Apply Time-based Window Function (Last Hour Transaction Count)
        // Window definition: partition by account_id, order by transaction timestamp, look back 3600 seconds (1 hour)
        WindowSpec timeWindow = Window.partitionBy(col("curr.account_id"))
                                     .orderBy(unix_timestamp(col("curr.trx_timestamp")))
                                     .rangeBetween(-3600, 0); // From 1 hour before to current row's timestamp
        LOGGER.info("Time-based window for transaction velocity defined.");

        // 7. Apply Complex Business Logic and Scoring
        LOGGER.info("Applying business logic and calculating fraud scores for each transaction.");
        Dataset<Row> scoredDf = dfFeatures
                .withColumn("trx_last_hour_cnt", count(col("curr.trx_id")).over(timeWindow))
                .withColumn("amount_z_score",
                    when(col("hist.stddev_trx_amount_90d").gt(lit(0.0)),
                         (col("curr.amount").minus(col("hist.avg_trx_amount_90d"))).divide(col("hist.stddev_trx_amount_90d")))
                    .otherwise(lit(0.0))
                )
                .withColumn("distance_risk_score",
                    when(col("distance_from_home_km").gt(lit(500.0)) // 500.0 for double comparison
                            .and(col("distance_from_home_km").notEqual(lit(-1.0))), lit(30))
                    .otherwise(lit(0))
                )
                .withColumn("amount_risk_score",
                    when(col("amount_z_score").gt(lit(3.0)), lit(40))
                    .when(col("amount_z_score").gt(lit(2.0)), lit(20))
                    .otherwise(lit(0))
                )
                .withColumn("velocity_risk_score",
                    when(col("trx_last_hour_cnt").gt(lit(5)), lit(30))
                    .when(col("trx_last_hour_cnt").gt(lit(3)), lit(15))
                    .otherwise(lit(0))
                )
                .withColumn("fraud_score",
                    col("distance_risk_score").plus(col("amount_risk_score")).plus(col("velocity_risk_score"))
                )
                .withColumn("is_fraud_alert", col("fraud_score").geq(lit(60)));
        LOGGER.info("Fraud scoring complete. Scores and alerts generated.");

        // 8. Select final columns and write to target Hive table
        LOGGER.info("Selecting final output columns for fin_mart.fraud_scores_daily.");
        Dataset<Row> finalOutput = scoredDf.select(
                col("curr.trx_id"), col("curr.account_id"), col("curr.amount"),
                col("distance_from_home_km"), col("amount_z_score"), col("trx_last_hour_cnt"),
                col("fraud_score"), col("is_fraud_alert"), lit(executionDate).as("scoring_date")
        );

        LOGGER.info("Writing final output to Hive table: fin_mart.fraud_scores_daily with append mode.");
        finalOutput.write()
                .mode("append")
                .insertInto("fin_mart.fraud_scores_daily");
        LOGGER.info("Fraud scoring for " + executionDate + " completed and results written successfully.");
    }

    public static void main(String[] args) {
        // Log basic startup information
        LOGGER.info("Starting FinCreditCardFraudScoring application.");

        if (args.length < 1) {
            // Log a severe error and provide usage instructions
            LOGGER.severe("Usage: spark-submit --class <your.package>.FinCreditCardFraudScoring " +
                          "--master yarn --deploy-mode client <your-jar-path>.jar <YYYY-MM-DD>");
            System.exit(1); // Exit with an error code
        }

        String execDate = args[0];
        SparkSession spark = null;
        try {
            spark = createSparkSession(); // Create SparkSession

            scoreTransactionsForFraud(spark, execDate); // Execute the scoring logic

            LOGGER.info("FinCreditCardFraudScoring application finished successfully for date: " + execDate);

        } catch (Exception e) {
            // Log any unhandled exceptions during the process as severe
            LOGGER.log(Level.SEVERE, "An unexpected error occurred during fraud scoring for date: " + execDate, e);
            System.exit(1); // Exit with an error code
        } finally {
            if (spark != null) {
                spark.stop(); // Stop SparkSession gracefully
                LOGGER.info("SparkSession stopped.");
            }
        }
    }
}
```
