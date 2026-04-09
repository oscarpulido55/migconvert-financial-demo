Okay, let's convert the given PySpark code to Java Spark.

Here's the breakdown of changes:
1.  **Imports**: Replace `pyspark.sql` imports with `org.apache.spark.sql` and static imports for `functions`.
2.  **`SparkSession` Configuration**: The `SparkSession.builder` syntax is very similar, just adjusting Java syntax for method calls.
3.  **DataFrame Operations**:
    *   `Dataset<Row>` is the Java equivalent of a PySpark DataFrame.
    *   `col("column_name") == value` becomes `col("column_name").equalTo(value)` for direct comparisons or `.gt()`, `.geq()`, `.lt()`, `.leq()`, `.and()`, `.or()` for conditions.
    *   `spark_max` and `spark_sum` from `pyspark.sql.functions` become `max` and `sum` when `org.apache.spark.sql.functions.*` is statically imported.
    *   `lit()` is used similarly.
    *   DataFrame aliasing like `df.alias("c")` becomes `df.as("c")`.
    *   Column aliasing within `select` or `agg` like `col("c").alias("new")` becomes `col("c").as("new")`.
    *   `Window` functions and `broadcast` hint are translated directly.
    *   `withColumnRenamed`, `drop`, `filter`, `groupBy`, `agg`, `select`, `withColumn` are all direct translations in terms of method names.
    *   `current_timestamp`, `year`, `month`, `dayofmonth`, `when`, `coalesce` are all directly available via static import from `org.apache.spark.sql.functions`.
    *   For the `when` clause in `withColumn`, string literals must be wrapped in `lit()`.
4.  **`main` method**: Standard Java class structure with a `public static void main(String[] args)` entry point.
5.  **Output and Logging**: `print()` statements are replaced with `System.out.println()`.

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec; // Specific import for WindowSpec

import static org.apache.spark.sql.functions.*; // Static import for all functions like col, lit, sum, max, etc.

public class CustomerOnboardingETL {

    /**
     * Initializes and returns a Spark session with Hive support enabled.
     */
    public static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("Financial_Customer_Onboarding_ETL")
                .enableHiveSupport()
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();
    }

    /**
     * Main ETL process for customer onboarding.
     * Reads raw customer data, KYC records, and initial funding details,
     * performs complex transformations, and writes to the dimensions table.
     * @param spark The SparkSession instance.
     */
    public static void processCustomerOnboarding(SparkSession spark) {
        // 1. Read Raw Data sources (Simulated paths in HDFS)
        System.out.println("Reading raw data sources...");
        Dataset<Row> rawCustomersDf = spark.read().json("hdfs://namenode:8020/landing/fin/customers/");
        Dataset<Row> rawKycDf = spark.read().parquet("hdfs://namenode:8020/landing/fin/kyc_status/");
        // For CSV, you need to explicitly set header and inferSchema options
        Dataset<Row> rawAccountsDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("hdfs://namenode:8020/landing/fin/accounts/");

        System.out.println("Extracting latest KYC status...");
        // 2. Extract latest KYC status using Window Functions
        WindowSpec kycWindow = Window.partitionBy("customer_id").orderBy(col("verification_date").desc());
        Dataset<Row> latestKycDf = rawKycDf
                .withColumn("row_num", max("verification_date").over(kycWindow)) // spark_max becomes max
                .filter(col("verification_date").equalTo(col("row_num"))) // Use .equalTo() for Column comparison
                .drop("row_num")
                .withColumnRenamed("status", "kyc_status")
                .withColumnRenamed("risk_rating", "kyc_risk_rating");

        System.out.println("Aggregating initial funding...");
        // 3. Aggregate Initial Funding
        Dataset<Row> fundingAggDf = rawAccountsDf
                .filter(col("account_status").equalTo("ACTIVE")) // Use .equalTo() for Column comparison
                .groupBy("customer_id")
                .agg(
                        sum("initial_deposit").as("total_initial_deposit"), // spark_sum becomes sum, .as() for alias
                        max("open_date").as("last_account_open_date")       // spark_max becomes max, .as() for alias
                );

        System.out.println("Joining and enriching data...");
        // 4. Join and Enrich
        // Broadcast join for smaller KYC dimension against larger Customers table
        Dataset<Row> enrichedCustomerDf = rawCustomersDf.as("c") // .alias() becomes .as() for Dataset
                .join(broadcast(latestKycDf).as("k"), col("c.customer_id").equalTo(col("k.customer_id")), "left_outer")
                .join(fundingAggDf.as("f"), col("c.customer_id").equalTo(col("f.customer_id")), "left_outer");

        System.out.println("Applying complex transformations...");
        // 5. Complex Transformations: Calculate customer segments and risk profiles
        Dataset<Row> finalDimCustomers = enrichedCustomerDf.select(
                col("c.customer_id"),
                col("c.first_name"),
                col("c.last_name"),
                col("c.ssn_hash").as("national_id_hash"), // .alias() becomes .as() for Column
                col("c.dob").as("date_of_birth"),
                col("c.address.country").as("country_code"),
                col("c.address.state").as("state_code"),
                coalesce(col("k.kyc_status"), lit("PENDING")).as("kyc_status"),
                coalesce(col("k.kyc_risk_rating"), lit("UNKNOWN")).as("risk_rating"),
                coalesce(col("f.total_initial_deposit"), lit(0.0)).as("total_initial_deposit"),
                col("f.last_account_open_date"),
                current_timestamp().as("etl_insert_ts")
        ).withColumn(
                "customer_segment",
                when(col("total_initial_deposit").gt(1000000.0), lit("PRIVATE_WEALTH")) // .gt() for greater than, lit() for string
                        .when(col("total_initial_deposit").geq(100000.0) // .geq() for greater than or equal to
                                .and(col("total_initial_deposit").leq(1000000.0)), // .and() and .leq() for less than or equal to
                                lit("PREMIUM"))
                        .when(col("kyc_status").equalTo("REJECTED"), lit("RESTRICTED")) // .equalTo() for equality
                        .otherwise(lit("RETAIL")) // lit() for string
        ).withColumn(
                "onboarding_year", year(col("last_account_open_date"))
        ).withColumn(
                "onboarding_month", month(col("last_account_open_date"))
        );

        System.out.println("Writing to Managed Hive Table...");
        // 6. Write to Managed Hive Table
        finalDimCustomers.write()
                .partitionBy("onboarding_year", "onboarding_month", "country_code")
                .format("orc")
                .mode("overwrite")
                .saveAsTable("fin_core.dim_customers");

        System.out.println("Successfully processed " + finalDimCustomers.count() + " customer records.");
    }

    public static void main(String[] args) {
        SparkSession spark = getSparkSession();

        // Optional: Setup DB for the demo
        spark.sql("CREATE DATABASE IF NOT EXISTS fin_core");

        processCustomerOnboarding(spark); // Call the process method

        spark.stop();
        System.out.println("Spark session stopped.");
    }
}
```