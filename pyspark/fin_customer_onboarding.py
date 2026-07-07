```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.Column;

public class FinancialCustomerOnboardingETL {

    public static SparkSession getSparkSession() {
        /**
         * Initializes and returns a Spark session.
         * For BigQuery integration, the 'spark-bigquery-with-dependencies' package is included.
         * Ensure your Spark environment is configured for GCP authentication (e.g., via Google Cloud SDK,
         * service account key JSON path, or workload identity).
         */
        return SparkSession.builder()
            .appName("Financial_Customer_Onboarding_ETL")
            // Configure BigQuery connector for Spark.
            // Replace '0.29.1' with the appropriate version compatible with your Spark version.
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.1")
            // Generic Spark SQL optimizations
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate();
    }

    public static void processCustomerOnboarding(SparkSession spark) {
        /**
         * Main ETL process for customer onboarding.
         * Reads raw customer data, KYC records, and initial funding details from GCS,
         * performs complex transformations, and writes to a BigQuery dimensions table.
         */
        // 1. Read Raw Data sources (Simulated paths in GCS)
        // Replace '[YOUR_GCS_LANDING_BUCKET]' with your actual GCS bucket name.
        Dataset<Row> rawCustomersDf = spark.read().json("gs://[YOUR_GCS_LANDING_BUCKET]/fin/customers/");
        Dataset<Row> rawKycDf = spark.read().parquet("gs://[YOUR_GCS_LANDING_BUCKET]/fin/kyc_status/");
        // For CSV, inferSchema should be explicitly handled or defined in Java if data quality is critical
        Dataset<Row> rawAccountsDf = spark.read().option("header", "true").option("inferSchema", "true").csv("gs://[YOUR_GCS_LANDING_BUCKET]/fin/accounts/");

        // 2. Extract latest KYC status using Window Functions
        Window kycWindow = Window.partitionBy("customer_id").orderBy(functions.col("verification_date").desc());
        Dataset<Row> latestKycDf = rawKycDf
            .withColumn("row_num", functions.max("verification_date").over(kycWindow))
            .filter(functions.col("verification_date").equalTo(functions.col("row_num")))
            .drop("row_num")
            .withColumnRenamed("status", "kyc_status")
            .withColumnRenamed("risk_rating", "kyc_risk_rating");

        // 3. Aggregate Initial Funding
        Dataset<Row> fundingAggDf = rawAccountsDf
            .filter(functions.col("account_status").equalTo("ACTIVE"))
            .groupBy("customer_id")
            .agg(
                functions.sum("initial_deposit").as("total_initial_deposit"),
                functions.max("open_date").as("last_account_open_date")
            );

        // 4. Join and Enrich
        // Broadcast join for smaller KYC dimension against larger Customers table
        Dataset<Row> enrichedCustomerDf = rawCustomersDf.as("c")
            .join(functions.broadcast(latestKycDf).as("k"), functions.col("c.customer_id").equalTo(functions.col("k.customer_id")), "left_outer")
            .join(fundingAggDf.as("f"), functions.col("c.customer_id").equalTo(functions.col("f.customer_id")), "left_outer");

        // 5. Complex Transformations: Calculate customer segments and risk profiles
        Dataset<Row> finalDimCustomers = enrichedCustomerDf.select(
            functions.col("c.customer_id"),
            functions.col("c.first_name"),
            functions.col("c.last_name"),
            functions.col("c.ssn_hash").as("national_id_hash"),
            functions.col("c.dob").as("date_of_birth"),
            functions.col("c.address.country").as("country_code"),
            functions.col("c.address.state").as("state_code"),
            functions.coalesce(functions.col("k.kyc_status"), functions.lit("PENDING")).as("kyc_status"),
            functions.coalesce(functions.col("k.kyc_risk_rating"), functions.lit("UNKNOWN")).as("risk_rating"),
            functions.coalesce(functions.col("f.total_initial_deposit"), functions.lit(0.0)).as("total_initial_deposit"),
            functions.col("f.last_account_open_date"),
            functions.current_timestamp().as("etl_insert_ts")
        ).withColumn(
            "customer_segment",
            functions.when(functions.col("total_initial_deposit").gt(1000000), "PRIVATE_WEALTH")
                .when(functions.col("total_initial_deposit").geq(100000).and(functions.col("total_initial_deposit").leq(1000000)), "PREMIUM")
                .when(functions.col("kyc_status").equalTo("REJECTED"), "RESTRICTED")
                .otherwise("RETAIL")
        ).withColumn(
            "onboarding_year", functions.year(functions.col("last_account_open_date"))
        ).withColumn(
            "onboarding_month", functions.month(functions.col("last_account_open_date"))
        );

        // 6. Write to BigQuery Table
        // The target is [YOUR_GCP_PROJECT_ID].fin_core.dim_customers.
        // BigQuery tables are partitioned differently than Hive. Ensure the target BigQuery table
        // is pre-created with appropriate partitioning (e.g., by DATE column `onboarding_year`
        // or `onboarding_month` if needed) or by ingestion time.
        // The `partitionBy` method used in Hive for dynamic partition creation is not directly
        // applicable here for BigQuery, as BigQuery partitioning is defined at table creation.
        // These columns will exist in the dataset written to BigQuery.
        // Replace '[YOUR_GCP_PROJECT_ID]' and '[YOUR_GCS_TEMP_BUCKET]' with actual values.
        finalDimCustomers.write()
            .format("bigquery")
            .mode("overwrite")
            .option("table", "[YOUR_GCP_PROJECT_ID].fin_core.dim_customers")
            // A temporary GCS bucket is required by the BigQuery connector for staging data.
            .option("temporaryGcsBucket", "[YOUR_GCS_TEMP_BUCKET]")
            .save();

        System.out.println(String.format("Successfully processed %d customer records.", finalDimCustomers.count()));
    }

    public static void main(String[] args) {
        SparkSession spark = getSparkSession();

        // Optional: Setup BigQuery dataset (equivalent to Hive database) for the demo
        // BigQuery uses datasets. The full dataset identifier includes the project ID.
        // Replace '[YOUR_GCP_PROJECT_ID]' with your actual GCP Project ID.
        // Note: CREATE SCHEMA is typically used in Spark 3.x for dataset/namespace operations.
        spark.sql("CREATE SCHEMA IF NOT EXISTS `[YOUR_GCP_PROJECT_ID]`.fin_core");

        processCustomerOnboarding(spark);
        spark.stop();
    }
}
```
