import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF5;
import static org.apache.spark.sql.functions.*;

import java.util.logging.Logger;
import java.util.logging.Level;

public class FinCreditCardFraudScoring {

    private static final Logger LOGGER = Logger.getLogger(FinCreditCardFraudScoring.class.getName());
    private static final String HAVERSINE_UDF_NAME = "haversineUDF";

    private static SparkSession createSparkSession() {
        LOGGER.info("Initializing Spark session...");
        SparkSession spark = SparkSession.builder()
                .appName("Financial_Credit_Card_Fraud_Scoring")
                .enableHiveSupport()
                .getOrCreate();
        LOGGER.info("Spark session initialized successfully.");
        return spark;
    }

    private static void registerHaversineUDF(SparkSession spark) {
        spark.udf().register(HAVERSINE_UDF_NAME, (UDF5<Double, Double, Double, Double, Double>) (lat1, lon1, lat2, lon2) -> {
            if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
                return -1.0;
            }
            double R = 6371.0; // Radius of earth in km
            double phi1 = Math.toRadians(lat1);
            double phi2 = Math.toRadians(lat2);
            double dphi = Math.toRadians(lat2 - lat1);
            double dlambda = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dphi / 2) * Math.sin(dphi / 2) +
                       Math.cos(phi1) * Math.cos(phi2) *
                       Math.sin(dlambda / 2) * Math.sin(dlambda / 2);
            return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        }, DataTypes.DoubleType);
        LOGGER.info("Haversine UDF registered with name: " + HAVERSINE_UDF_NAME);
    }

    private static void scoreTransactionsForFraud(SparkSession spark, String executionDate) {
        LOGGER.info("Starting fraud scoring for execution date: " + executionDate);

        // 1. Load Data
        LOGGER.info("Loading financial data...");
        Dataset<Row> fullCcTrx = spark.table("fin_core.cc_transactions");
        Dataset<Row> accounts = spark.table("fin_core.dim_accounts");
        Dataset<Row> merchants = spark.table("fin_core.dim_merchants");
        LOGGER.info("Financial data loaded.");

        // 2. Extract current day transactions
        LOGGER.info("Filtering current day transactions...");
        Dataset<Row> ccTrx = fullCcTrx.filter(col("trx_date").equalTo(lit(executionDate)));
        LOGGER.info("Current day transactions filtered.");

        // 3. Join location data and calculate distance Native
        LOGGER.info("Joining location data and calculating distances...");
        Dataset<Row> enrichedTrx = ccTrx.alias("t")
                .join(accounts.alias("a"), col("t.account_id").equalTo(col("a.account_id")), "inner")
                .join(merchants.alias("m"), col("t.merchant_id").equalTo(col("m.merchant_id")), "left")
                .withColumn(
                    "distance_from_home_km",
                    callUDF(HAVERSINE_UDF_NAME,
                            col("a.home_lat"), col("a.home_lon"),
                            col("m.merchant_lat"), col("m.merchant_lon"))
                );
        LOGGER.info("Location data joined and distances calculated.");

        // 4. Pure DataFrame Historical Profiling Window
        LOGGER.info("Building historical profiles...");
        // Filter for the last 90 days of transactions (excluding execution date)
        Dataset<Row> histTrx = fullCcTrx.filter(
            col("trx_date").geq(date_sub(lit(executionDate), 90))
            .and(col("trx_date").leq(date_sub(lit(executionDate), 1)))
        );

        // Aggregate to build the historical profile
        Dataset<Row> histProfile = histTrx.groupBy("account_id").agg(
            avg("amount").alias("avg_trx_amount_90d"),
            stddev_samp("amount").alias("stddev_trx_amount_90d"),
            (count("trx_id").divide(lit(90.0))).alias("avg_daily_trx_count")
        ).na().fill(0.0, new String[]{"stddev_trx_amount_90d"});
        LOGGER.info("Historical profiles built.");

        // 5. Join current day transactions with their historical profiles
        LOGGER.info("Joining current day transactions with historical profiles...");
        Dataset<Row> dfFeatures = enrichedTrx.alias("curr")
                .join(histProfile.alias("hist"), col("curr.account_id").equalTo(col("hist.account_id")), "left");
        LOGGER.info("Joined current transactions with historical profiles.");

        // 6. Apply Time-based Window Function (Last Hour Trx Count)
        LOGGER.info("Applying time-based window function for last hour transaction count...");
        Window timeWindow = Window.partitionBy(col("curr.account_id")).orderBy(unix_timestamp(col("curr.trx_timestamp"))).rangeBetween(-3600, 0);

        // 7. Apply Complex Business Logic and Scoring Native DataFrame API
        LOGGER.info("Applying business logic and calculating fraud scores...");
        Dataset<Row> scoredDf = dfFeatures
                .withColumn("trx_last_hour_cnt", count(col("curr.trx_id")).over(timeWindow))
                .withColumn("amount_z_score",
                    when(col("hist.stddev_trx_amount_90d").gt(lit(0.0)),
                         (col("curr.amount").minus(col("hist.avg_trx_amount_90d"))).divide(col("hist.stddev_trx_amount_90d")))
                    .otherwise(lit(0.0))
                )
                .withColumn("distance_risk_score",
                    when(col("distance_from_home_km").gt(lit(500.0)).and(col("distance_from_home_km").notEqual(lit(-1.0))), lit(30))
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
        LOGGER.info("Fraud scores calculated.");

        // 8. Select final columns and write to target
        LOGGER.info("Selecting final columns and writing to target table: fin_mart.fraud_scores_daily");
        Dataset<Row> finalOutput = scoredDf.select(
            col("curr.trx_id"), col("curr.account_id"), col("curr.amount"),
            col("distance_from_home_km"), col("amount_z_score"), col("trx_last_hour_cnt"),
            col("fraud_score"), col("is_fraud_alert"), lit(executionDate).alias("scoring_date")
        );

        finalOutput.write()
            .mode("append")
            .insertInto("fin_mart.fraud_scores_daily");
        LOGGER.info("Pure DataFrame Fraud scoring completed for " + executionDate);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            LOGGER.severe("Usage: spark-submit --class <your.package.FinCreditCardFraudScoring> --master yarn --deploy-mode cluster <your-jar-file.jar> <YYYY-MM-DD>");
            System.exit(1);
        }

        String execDate = args[0];
        SparkSession spark = null;
        try {
            spark = createSparkSession();
            registerHaversineUDF(spark);
            scoreTransactionsForFraud(spark, execDate);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "An error occurred during fraud scoring for " + execDate, e);
            System.exit(1);
        } finally {
            if (spark != null) {
                spark.stop();
                LOGGER.info("Spark session stopped.");
            }
        }
    }
}
