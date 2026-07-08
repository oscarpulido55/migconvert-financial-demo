```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.DataTypes; // Used for DoubleType
import scala.collection.JavaConverters;

import static org.apache.spark.sql.functions.*;

public class FinancialCreditCardFraudScoring {

    public static SparkSession createSparkSession() {
        /**
         * Initializes and returns a Spark session.
         * Configuration for BigQuery access needs to be added here,
         * e.g., spark.jars for the BigQuery connector and project ID.
         */
        return SparkSession.builder()
                .appName("Financial_Credit_Card_Fraud_Scoring")
                // For BigQuery, Hive support is generally not needed for accessing BigQuery tables directly.
                // Instead, configure the Spark BigQuery connector.
                // .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.1") // Example BigQuery connector
                // .config("spark.cloud.google.project.id", "your-gcp-project-id") // Specify your GCP Project ID
                .getOrCreate();
    }

    // Create a custom UDF for great circle distance
    /**
     * Calculates the great circle distance between two points on the earth.
     * Converted from Python's haversine function.
     */
    public static double haversine(Double lat1, Double lon1, Double lat2, Double lon2) {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
            return -1.0;
        }
        double R = 6371.0; // Radius of earth in km
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dphi = Math.toRadians(lat2 - lat1);
        double dlambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dphi / 2) * Math.sin(dphi / 2) +
                   Math.cos(phi1) * Math.cos(phi2) * Math.sin(dlambda / 2) * Math.sin(dlambda / 2);
        return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // Register the UDF
    private static UserDefinedFunction haversineUdf = udf(
            (Double lat1, Double lon1, Double lat2, Double lon2) -> haversine(lat1, lon1, lat2, lon2),
            DataTypes.DoubleType
    );

    /**
     * Uses pure Spark DataFrame APIs to score credit card transactions.
     * Replaces embedded SQL with continuous DataFrame transformations.
     */
    public static void scoreTransactionsForFraud(SparkSession spark, String executionDate) {

        // 1. Load Data
        // Note: For BigQuery, ensure 'fin_core' is a dataset and tables are within it.
        // If not using Spark's BigQuery catalog, you might need to use
        // spark.read().format("bigquery").option("table", "project:dataset.table").load()
        Dataset<Row> fullCcTrx = spark.table("fin_core.cc_transactions"); // Assumes fin_core.cc_transactions is resolved correctly by Spark-BigQuery connector
        Dataset<Row> accounts = spark.table("fin_core.dim_accounts");     // Assumes fin_core.dim_accounts is resolved correctly by Spark-BigQuery connector
        Dataset<Row> merchants = spark.table("fin_core.dim_merchants");   // Assumes fin_core.dim_merchants is resolved correctly by Spark-BigQuery connector

        // 2. Extract current day transactions
        Dataset<Row> ccTrx = fullCcTrx.filter(col("trx_date").equalTo(executionDate));

        // 3. Join location data and calculate distance
        Dataset<Row> enrichedTrx = ccTrx.alias("t")
                .join(accounts.alias("a"), col("t.account_id").equalTo(col("a.account_id")), "inner")
                .join(merchants.alias("m"), col("t.merchant_id").equalTo(col("m.merchant_id")), "left_outer")
                .withColumn(
                    "distance_from_home_km",
                    haversineUdf.apply(col("a.home_lat"), col("a.home_lon"), col("m.merchant_lat"), col("m.merchant_lon"))
                );

        // 4. Pure DataFrame Historical Profiling Window
        // Filter for the last 90 days of transactions (excluding execution date)
        Dataset<Row> histTrx = fullCcTrx.filter(
            col("trx_date").geq(date_sub(lit(executionDate), 90)).and(
            col("trx_date").leq(date_sub(lit(executionDate), 1)))
        );

        // Aggregate to build the historical profile
        Dataset<Row> histProfile = histTrx.groupBy("account_id").agg(
            avg("amount").as("avg_trx_amount_90d"),
            stddev_samp("amount").as("stddev_trx_amount_90d"),
            (count("trx_id").divide(90.0)).as("avg_daily_trx_count")
        ).na().fill(0.0, new String[]{"stddev_trx_amount_90d"});

        // 5. Join current day transactions with their historical profiles
        Dataset<Row> dfFeatures = enrichedTrx.alias("curr")
                .join(histProfile.alias("hist"), col("curr.account_id").equalTo(col("hist.account_id")), "left_outer");

        // 6. Apply Time-based Window Function (Last Hour Trx Count)
        Window timeWindow = Window.partitionBy(col("curr.account_id")).orderBy(unix_timestamp(col("curr.trx_timestamp"))).rangeBetween(-3600, 0);

        // 7. Apply Complex Business Logic and Scoring
        Dataset<Row> scoredDf = dfFeatures
                .withColumn("trx_last_hour_cnt", count(col("curr.trx_id")).over(timeWindow))
                .withColumn("amount_z_score",
                    when(col("hist.stddev_trx_amount_90d").gt(0),
                         (col("curr.amount").minus(col("hist.avg_trx_amount_90d"))).divide(col("hist.stddev_trx_amount_90d")))
                    .otherwise(lit(0.0))
                )
                .withColumn("distance_risk_score",
                    when(col("distance_from_home_km").gt(500).and(col("distance_from_home_km").notEqual(-1.0)), lit(30)).otherwise(lit(0))
                )
                .withColumn("amount_risk_score",
                    when(col("amount_z_score").gt(3.0), lit(40))
                    .when(col("amount_z_score").gt(2.0), lit(20))
                    .otherwise(lit(0))
                )
                .withColumn("velocity_risk_score",
                    when(col("trx_last_hour_cnt").gt(5), lit(30))
                    .when(col("trx_last_hour_cnt").gt(3), lit(15))
                    .otherwise(lit(0))
                )
                .withColumn("fraud_score",
                    col("distance_risk_score").plus(col("amount_risk_score")).plus(col("velocity_risk_score"))
                )
                .withColumn("is_fraud_alert", col("fraud_score").geq(60));

        // 8. Select final columns and write to target
        Dataset<Row> finalOutput = scoredDf.select(
            col("curr.trx_id"), col("curr.account_id"), col("curr.amount"),
            col("distance_from_home_km"), col("amount_z_score"), col("trx_last_hour_cnt"),
            col("fraud_score"), col("is_fraud_alert"), lit(executionDate).as("scoring_date")
        );

        // Write mode is append. For BigQuery, ensure the target table
        // 'fin_mart.fraud_scores_daily' exists and has a compatible schema.
        // Using insertInto implies that Spark is configured to recognize
        // 'fin_mart.fraud_scores_daily' as a target BigQuery table.
        // Alternative and often more explicit for BigQuery:
        // finalOutput.write()
        //     .format("bigquery")
        //     .option("temporaryGcsBucket", "your-gcs-bucket-for-spark-temp-data")
        //     .option("table", "your-gcp-project:fin_mart.fraud_scores_daily")
        //     .mode(org.apache.spark.sql.SaveMode.Append)
        //     .save();
        finalOutput.write()
            .mode("append")
            .insertInto("fin_mart.fraud_scores_daily"); // Assumes BigQuery target is managed through Spark catalog or configuration

        System.out.println(String.format("Pure DataFrame Fraud scoring completed for %s", executionDate));
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: spark-submit FinancialCreditCardFraudScoring.jar <YYYY-MM-DD>");
            System.exit(1);
        }

        String execDate = args[0];
        SparkSession spark = createSparkSession();

        try {
            scoreTransactionsForFraud(spark, execDate);
        } finally {
            spark.stop();
        }
    }
}
```
