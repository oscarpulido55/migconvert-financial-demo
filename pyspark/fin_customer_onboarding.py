```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

// Use SLF4J for logging in Java, replacing Python's print statements.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinancialCustomerOnboardingETL {

    // Logger instance for this class.
    private static final Logger LOGGER = LoggerFactory.getLogger(FinancialCustomerOnboardingETL.class);

    /**
     * Initializes and returns a Spark session for BigQuery processing.
     * Hive support is not applicable for BigQuery, so `.enableHiveSupport()` is removed.
     * BigQuery connector specific configurations are added.
     *
     * @return A configured SparkSession.
     */
    public static SparkSession getSparkSession() {
        // Replace 'your-gcp-project' and 'your-gcs-temp-bucket' with actual GCP project ID and a GCS bucket name.
        // The GCS temporary bucket is required by the Spark BigQuery connector for staging data.
        return SparkSession.builder()
                .appName("Financial_Customer_Onboarding_ETL_BigQuery")
                // .enableHiveSupport() - Removed as Hive is not used with BigQuery.
                // Standard Spark configurations for performance.
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                // BigQuery specific configurations for the Spark connector.
                // The 'spark-bigquery-with-dependencies' package provides necessary BigQuery client libraries.
                // The version (_2.13) should match your Spark Scala version.
                .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.29.0")
                // Sets the default GCP project for the connector. Can also be set via environment variable GOOGLE_CLOUD_PROJECT.
                .config("spark.cloud.google.project.id", "your-gcp-project")
                // Configures Hadoop to use Google Cloud Storage connector.
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.gs.project.id", "your-gcp-project")
                // Enables service account authentication; ensure your Spark environment has credentials configured (e.g., ADC, or mounted JSON key).
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                // Specifies a GCS bucket for temporary staging by the BigQuery connector during read/write operations.
                .config("temporaryGcsBucket", "your-gcs-temp-bucket")
                // Recommended for flexible date/timestamp parsing when inferring schema from various source formats.
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate();
    }

    /**
     * Main ETL process for customer onboarding.
     * Reads raw customer data from GCS, KYC records, and initial funding details,
     * performs complex transformations, and writes the result to a BigQuery dimensions table.
     *
     * @param spark The SparkSession instance to use.
     */
    public static void processCustomerOnboarding(SparkSession spark) {
        // 1. Read Raw Data sources. Original HDFS paths are converted to GCS paths for cloud context.
        // Replace 'gs://your-gcs-bucket/' with the actual GCS path to your landing zone.
        LOGGER.info("Reading raw customer data from GCS...");
        Dataset<Row> rawCustomersDf = spark.read().json("gs://your-gcs-bucket/landing/fin/customers/");
        Dataset<Row> rawKycDf = spark.read().parquet("gs://your-gcs-bucket/landing/fin/kyc_status/");
        // For CSV, ensure header and schema inference options are correctly applied.
        Dataset<Row> rawAccountsDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("gs://your-gcs-bucket/landing/fin/accounts/");

        // 2. Extract latest KYC status using Window Functions.
        LOGGER.info("Extracting latest KYC status using window functions...");
        Window kycWindow = Window.partitionBy("customer_id").orderBy(col("verification_date").desc());
        Dataset<Row> latestKycDf = rawKycDf
                // Using 'functions.max' or static import 'max()' from org.apache.spark.sql.functions.
                .withColumn("row_num", max("verification_date").over(kycWindow))
                // For Column comparisons in Java, use '.equalTo()' instead of '=='.
                .filter(col("verification_date").equalTo(col("row_num")))
                .drop("row_num")
                .withColumnRenamed("status", "kyc_status")
                .withColumnRenamed("risk_rating", "kyc_risk_rating");

        // 3. Aggregate Initial Funding.
        LOGGER.info("Aggregating initial funding...");
        Dataset<Row> fundingAggDf = rawAccountsDf
                .filter(col("account_status").equalTo("ACTIVE"))
                .groupBy("customer_id")
                .agg(
                        sum("initial_deposit").as("total_initial_deposit"), // Use .as() for alias
                        max("open_date").as("last_account_open_date")
                );

        // 4. Join and Enrich data.
        // Broadcast join is applied to the smaller KYC dimension table against the larger customers table.
        LOGGER.info("Joining and enriching customer data...");
        Dataset<Row> enrichedCustomerDf = rawCustomersDf.as("c") // Use .as() for alias in Java Spark
                // For Column comparisons in join conditions, use '.equalTo()'.
                .join(broadcast(latestKycDf).as("k"), col("c.customer_id").equalTo(col("k.customer_id")), "left_outer")
                .join(fundingAggDf.as("f"), col("c.customer_id").equalTo(col("f.customer_id")), "left_outer");

        // 5. Complex Transformations: Calculate customer segments and risk profiles.
        LOGGER.info("Applying complex transformations and calculating customer segments...");
        Dataset<Row> finalDimCustomers = enrichedCustomerDf.select(
                col("c.customer_id"),
                col("c.first_name"),
                col("c.last_name"),
                col("c.ssn_hash").as("national_id_hash"),
                col("c.dob").as("date_of_birth"),
                col("c.address.country").as("country_code"),
                col("c.address.state").as("state_code"),
                coalesce(col("k.kyc_status"), lit("PENDING")).as("kyc_status"), // Use lit() for literals
                coalesce(col("k.kyc_risk_rating"), lit("UNKNOWN")).as("risk_rating"),
                coalesce(col("f.total_initial_deposit"), lit(0.0)).as("total_initial_deposit"),
                col("f.last_account_open_date"),
                current_timestamp().as("etl_insert_ts")
        ).withColumn(
                "customer_segment",
                // For comparison operators on Columns, use methods like gt(), geq(), leq().
                // For boolean 'and', use .and(). For string literals in conditions, use lit().
                when(col("total_initial_deposit").gt(1000000), lit("PRIVATE_WEALTH"))
                .when(col("total_initial_deposit").geq(100000).and(col("total_initial_deposit").leq(1000000)), lit("PREMIUM"))
                .when(col("kyc_status").equalTo(lit("REJECTED")), lit("RESTRICTED"))
                .otherwise(lit("RETAIL"))
        ).withColumn(
                "onboarding_year", year(col("last_account_open_date"))
        ).withColumn(
                "onboarding_month", month(col("last_account_open_date"))
        );

        // 6. Write to BigQuery Table.
        // The target BigQuery table is 'your-gcp-project.fin_core.dim_customers'.
        // BigQuery datasets (like 'fin_core') need to be created manually or via
        // Google Cloud client libraries before running this Spark job.
        // Replace 'your-gcp-project', 'fin_core', 'dim_customers', and 'your-gcs-temp-bucket' with actual values.
        LOGGER.info("Writing processed data to BigQuery table 'your-gcp-project.fin_core.dim_customers'...");
        finalDimCustomers.write()
                .format("bigquery")
                .option("table", "your-gcp-project.fin_core.dim_customers") // Specifies the target BigQuery table.
                .option("temporaryGcsBucket", "your-gcs-temp-bucket") // Required for intermediate GCS staging.
                .option("writeDisposition", "WRITE_TRUNCATE") // Corresponds to Python's .mode("overwrite").
                // While Spark's partitionBy() in PySpark used to dictate HDFS file layout for Hive,
                // for BigQuery, its impact varies. It often influences how data is written to the
                // temporary GCS bucket and can guide BigQuery table clustering for the specified columns.
                // For native BigQuery partitioning (e.g., by date), the BigQuery table schema
                // must be defined with partitioning enabled.
                .partitionBy("onboarding_year", "onboarding_month", "country_code")
                .save();

        // Logging the number of processed records, replacing Python's f-string print.
        LOGGER.info("Successfully processed {} customer records.", finalDimCustomers.count());
    }

    public static void main(String[] args) {
        SparkSession spark = null;
        try {
            spark = getSparkSession();

            // The original 'CREATE DATABASE IF NOT EXISTS fin_core' DDL statement is specific to Hive.
            // BigQuery uses "datasets" instead of "databases". Datasets like 'fin_core'
            // must be pre-created in Google Cloud Project before the Spark job runs,
            // as the Spark BigQuery connector expects the dataset to exist.
            LOGGER.warn("The original 'CREATE DATABASE IF NOT EXISTS fin_core' DDL for Hive is not applicable to BigQuery.");
            LOGGER.warn("Please ensure the BigQuery dataset 'fin_core' exists in 'your-gcp-project' before execution.");

            processCustomerOnboarding(spark);
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }
}
```
