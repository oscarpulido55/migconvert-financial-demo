import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import java.io.Serializable;
import java.lang.Math;

public class FinancialCreditCardFraudScoring implements Serializable {

    // UDF implementation for haversine distance
    private static class HaversineFunction implements org.apache.spark.sql.api.java.UDF4<Double, Double, Double, Double, Double>, Serializable {
        @Override
        public Double call(Double lat1, Double lon1, Double lat2, Double lon2) throws Exception {
            if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
                return -1.0;
            }
            double R = 6371.0; // Radius of earth in km
            double phi1 = Math.toRadians(lat1);
            double phi2 = Math.toRadians(lat2);
            double dphi = Math.toRadians(lat2 - lat1);
            double dlambda = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dphi/2) * Math.sin(dphi/2) + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dlambda/2) * Math.sin(dlambda/2);
            return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        }
    }

    private static UserDefinedFunction haversineUDF = udf(new HaversineFunction(), DataTypes.DoubleType);

    public static SparkSession createSparkSession(String bigqueryProjectId) {
        // Configure Spark to use the BigQuery connector
        // Ensure 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.0' (or compatible version)
        // is available on the classpath or passed via spark.jars.packages.
        return SparkSession.builder()
            .appName("Financial_Credit_Card_Fraud_Scoring")
            // Removed .enableHiveSupport() as the target database is BigQuery, not Hive.
            // Configured for BigQuery project ID.
            .config("spark.cloud.google.project.id", bigqueryProjectId)
            .getOrCreate();
    }

    public static void scoreTransactionsForFraud(SparkSession spark, String executionDate, String bigqueryProjectId) {

        // 1. Load Data from BigQuery
        // Replaced spark.table() with BigQuery connector specific read operations
        Dataset<Row> fullCcTrx = spark.read()
            .format("bigquery")
            .option("table", String.format("%s:fin_core.cc_transactions", bigqueryProjectId))
            .load();

        Dataset<Row> accounts = spark.read()
            .format("bigquery")
            .option("table", String.format("%s:fin_core.dim_accounts", bigqueryProjectId))
            .load();

        Dataset<Row> merchants = spark.read()
            .format("bigquery")
            .option("table", String.format("%s:fin_core.dim_merchants", bigqueryProjectId))
            .load();

        // 2. Extract current day transactions
        Dataset<Row> ccTrx = fullCcTrx.filter(col("trx_date").equalTo(executionDate));

        // 3. Join location data and calculate distance Native
        Dataset<Row> enrichedTrx = ccTrx.as("t")
            .join(accounts.as("a"), col("t.account_id").equalTo(col("a.account_id")), "inner")
            .join(merchants.as("m"), col("t.merchant_id").equalTo(col("m.merchant_id")), "left")
            .withColumn(
                "distance_from_home_km",
                haversineUDF.apply(col("a.home_lat"), col("a.home_lon"), col("m.merchant_lat"), col("m.merchant_lon"))
            );

        // 4. Pure DataFrame Historical Profiling Window
        // Filter for the last 90 days of transactions (excluding execution date)
        Dataset<Row> histTrx = fullCcTrx.filter(
            col("trx_date").geq(date_sub(lit(executionDate), 90))
                .and(col("trx_date").leq(date_sub(lit(executionDate), 1)))
        );

        // Aggregate to build the historical profile
        Dataset<Row> histProfile = histTrx.groupBy("account_id").agg(
            avg("amount").as("avg_trx_amount_90d"),
            stddev_samp("amount").as("stddev_trx_amount_90d"),
            (count("trx_id").divide(lit(90.0))).as("avg_daily_trx_count")
        ).na().fill(0.0, new String[]{"stddev_trx_amount_90d"});

        // 5. Join current day transactions with their historical profiles
        Dataset<Row> dfFeatures = enrichedTrx.as("curr")
            .join(histProfile.as("hist"), col("curr.account_id").equalTo(col("hist.account_id")), "left");

        // 6. Apply Time-based Window Function (Last Hour Trx Count)
        WindowSpec timeWindow = Window.partitionBy(col("curr.account_id"))
                                    .orderBy(unix_timestamp(col("curr.trx_timestamp")))
                                    .rangeBetween(-3600, 0);

        // 7. Apply Complex Business Logic and Scoring Native DataFrame API
        Dataset<Row> scoredDf = dfFeatures
            .withColumn("trx_last_hour_cnt", count(col("curr.trx_id")).over(timeWindow))
            .withColumn("amount_z_score",
                when(col("hist.stddev_trx_amount_90d").gt(0),
                     (col("curr.amount").minus(col("hist.avg_trx_amount_90d"))).divide(col("hist.stddev_trx_amount_90d")))
                .otherwise(lit(0.0))
            )
            .withColumn("distance_risk_score",
                when(col("distance_from_home_km").gt(500)
                     .and(col("distance_from_home_km").notEqual(lit(-1.0))), lit(30))
                .otherwise(lit(0))
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
            .withColumn("is_fraud_alert", col("fraud_score").geq(lit(60)));

        // 8. Select final columns and write to target
        Dataset<Row> finalOutput = scoredDf.select(
            col("curr.trx_id"), col("curr.account_id"), col("curr.amount"),
            col("distance_from_home_km"), col("amount_z_score"), col("trx_last_hour_cnt"),
            col("fraud_score"), col("is_fraud_alert"), lit(executionDate).as("scoring_date")
        );

        // Write to BigQuery using the connector
        finalOutput.write()
            .format("bigquery")
            .option("table", String.format("%s:fin_mart.fraud_scores_daily", bigqueryProjectId))
            .mode("append")
            .save();

        System.out.println(String.format("Pure DataFrame Fraud scoring completed for %s", executionDate));
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: spark-submit --class FinancialCreditCardFraudScoring <jar_path> <YYYY-MM-DD> <GCP_PROJECT_ID>");
            System.exit(1);
        }

        String execDate = args[0];
        String bigqueryProjectId = args[1]; // Expecting GCP Project ID as the second argument

        SparkSession sp = createSparkSession(bigqueryProjectId);

        scoreTransactionsForFraud(sp, execDate, bigqueryProjectId);
        sp.stop();
    }
}
