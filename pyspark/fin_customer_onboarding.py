```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

public class FinancialCustomerOnboardingETL {

    public static SparkSession getSparkSession() {
        /**
         * Initializes and returns a Spark session with BigQuery connector support.
         * Note: Removed enableHiveSupport as we are targeting BigQuery.
         * Ensure 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12' jar
         * (or appropriate Scala version) is in the classpath or configured via spark.jars.packages.
         * The 'temporaryGcsBucket' option for BigQuery writes must be configured either in the session or during write operations.
         */
        return SparkSession.builder()
            .appName("Financial_Customer_Onboarding_ETL")
            // .enableHiveSupport() // Removed: Not applicable for BigQuery in this context
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            // Optional: Configure Google Cloud Project and GCS temp bucket for BigQuery connector.
            // When running on GCP services like Dataproc, these are often inferred.
            // .config("spark.cloud.google.project.id", "your-gcp-project-id")
            // .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            // .config("spark.hadoop.google.cloud.auth.service.account.json_keyfile", "/path/to/your/service_account_key.json")
            .getOrCreate();
    }

    public static void processCustomerOnboarding(SparkSession spark, String gcsBucketName, String gcpProjectId) {
        /**
         * Main ETL process for customer onboarding.
         * Reads raw customer data, KYC records, and initial funding details,
         * performs complex transformations, and writes to the dimensions table in BigQuery.
         *
         * @param spark SparkSession object
         * @param gcsBucketName The GCS bucket name to use for landing zone data and temporary BigQuery staging.
         * @param gcpProjectId The Google Cloud Project ID where BigQuery datasets reside.
         */
        // 1. Read Raw Data sources (Simulated paths in GCS)
        // Original HDFS paths are converted to Google Cloud Storage (GCS) paths.
        Dataset<Row> rawCustomersDf = spark.read().json(String.format("gs://%s/landing/fin/customers/", gcsBucketName));
        Dataset<Row> rawKycDf = spark.read().parquet(String.format("gs://%s/landing/fin/kyc_status/", gcsBucketName));
        Dataset<Row> rawAccountsDf = spark.read().option("header", "true").option("inferSchema", "true")
                                          .csv(String.format("gs://%s/landing/fin/accounts/", gcsBucketName));

        // 2. Extract latest KYC status using Window Functions
        org.apache.spark.sql.expressions.WindowSpec kycWindow = Window.partitionBy(functions.col("customer_id"))
            .orderBy(functions.col("verification_date").desc());
        // Original PySpark used `spark_max("verification_date").over(kyc_window)` to filter for the latest.
        // Using `row_number()` and filtering for rank 1 is a more common and direct equivalent.
        Dataset<Row> latestKycDf = rawKycDf
            .withColumn("row_num", functions.row_number().over(kycWindow))
            .filter(functions.col("row_num").equalTo(functions.lit(1)))
            .drop("row_num")
            .withColumnRenamed("status", "kyc_status")
            .withColumnRenamed("risk_rating", "kyc_risk_rating");

        // 3. Aggregate Initial Funding
        Dataset<Row> fundingAggDf = rawAccountsDf
            .filter(functions.col("account_status").equalTo("ACTIVE"))
            .groupBy(functions.col("customer_id"))
            .agg(
                functions.sum("initial_deposit").alias("total_initial_deposit"),
                functions.max("open_date").alias("last_account_open_date")
            );

        // 4. Join and Enrich
        // Broadcast join for smaller KYC dimension against larger Customers table
        Dataset<Row> enrichedCustomerDf = rawCustomersDf.alias("c")
            .join(functions.broadcast(latestKycDf).alias("k"), functions.col("c.customer_id").equalTo(functions.col("k.customer_id")), "left_outer")
            .join(fundingAggDf.alias("f"), functions.col("c.customer_id").equalTo(functions.col("f.customer_id")), "left_outer");

        // 5. Complex Transformations: Calculate customer segments and risk profiles
        Dataset<Row> finalDimCustomers = enrichedCustomerDf.select(
            functions.col("c.customer_id"),
            functions.col("c.first_name"),
            functions.col("c.last_name"),
            functions.col("c.ssn_hash").alias("national_id_hash"),
            functions.col("c.dob").alias("date_of_birth"),
            functions.col("c.address.country").alias("country_code"),
            functions.col("c.address.state").alias("state_code"),
            functions.coalesce(functions.col("k.kyc_status"), functions.lit("PENDING")).alias("kyc_status"),
            functions.coalesce(functions.col("k.kyc_risk_rating"), functions.lit("UNKNOWN")).alias("risk_rating"),
            functions.coalesce(functions.col("f.total_initial_deposit"), functions.lit(0.0)).alias("total_initial_deposit"),
            functions.col("f.last_account_open_date"),
            functions.current_timestamp().alias("etl_insert_ts")
        ).withColumn(
            "customer_segment",
            functions.when(functions.col("total_initial_deposit").gt(1000000), functions.lit("PRIVATE_WEALTH"))
            .when(functions.col("total_initial_deposit").geq(100000).and(functions.col("total_initial_deposit").leq(1000000)), functions.lit("PREMIUM"))
            .when(functions.col("kyc_status").equalTo("REJECTED"), functions.lit("RESTRICTED"))
            .otherwise(functions.lit("RETAIL"))
        ).withColumn(
            "onboarding_year", functions.year(functions.col("last_account_open_date"))
        ).withColumn(
            "onboarding_month", functions.month(functions.col("last_account_open_date"))
        );

        // 6. Write to BigQuery Table
        // The target is fin_core.dim_customers. BigQuery tables are not typically partitioned dynamically
        // by multiple string/integer columns in the same way Hive does using `partitionBy` for physical directories.
        // If native BigQuery partitioning is desired (e.g., by a DATE column for 'last_account_open_date'),
        // the BigQuery table 'dim_customers' should be pre-created with the desired partitioning scheme.
        // Spark will write data to a temporary GCS location first, then load it into BigQuery.
        finalDimCustomers.write()
            .format("bigquery")
            .option("temporaryGcsBucket", gcsBucketName) // A GCS bucket for staging data
            .option("table", String.format("%s:fin_core.dim_customers", gcpProjectId)) // Target BigQuery table: project_id:dataset_id.table_id
            .mode(SaveMode.Overwrite) // Overwrite existing table data
            .save();

        System.out.println(String.format("Successfully processed %d customer records.", finalDimCustomers.count()));
    }

    public static void main(String[] args) {
        // --- IMPORTANT: Configuration for BigQuery Integration ---
        // You MUST provide a GCS bucket for temporary BigQuery staging and your GCP Project ID.
        // Example of setting these via environment variables (recommended for production):
        // export GCS_BUCKET_NAME=your-spark-temp-gcs-bucket
        // export GCP_PROJECT_ID=your-gcp-project-id
        String gcsBucketName = System.getenv("GCS_BUCKET_NAME");
        String gcpProjectId = System.getenv("GCP_PROJECT_ID");

        if (gcsBucketName == null || gcsBucketName.isEmpty() || gcpProjectId == null || gcpProjectId.isEmpty()) {
            System.err.println("Error: GCS_BUCKET_NAME and GCP_PROJECT_ID environment variables must be set for BigQuery integration.");
            System.err.println("Usage: To run, set environment variables or pass as program arguments. E.g., via spark-submit.");
            System.exit(1);
        }

        SparkSession spark = getSparkSession();

        // Optional: Setup DB for the demo
        // Original PySpark: spark.sql("CREATE DATABASE IF NOT EXISTS fin_core")
        // BigQuery does not have 'CREATE DATABASE'. The equivalent is 'CREATE SCHEMA' (for datasets).
        // While Spark SQL can interact with some BigQuery DDL, dataset creation through `spark.sql`
        // with the BigQuery connector is not the primary or guaranteed way to create datasets.
        // It's usually handled via BigQuery API, gcloud CLI, or console.
        // This line is a conceptual conversion; ensure 'fin_core' dataset exists in BigQuery.
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.fin_core", gcpProjectId)); // Conceptual DDL

        processCustomerOnboarding(spark, gcsBucketName, gcpProjectId);
        spark.stop();
    }
}
```
