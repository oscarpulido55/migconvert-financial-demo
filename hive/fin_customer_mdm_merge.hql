Converting a Hive SQL (HQL) script to Java typically involves using a data processing framework like **Apache Spark** or **Apache Flink** with their respective SQL APIs or DataFrame/Dataset APIs.

The HQL script describes an SCD Type 2 merge. I'll provide a Java solution using **Apache Spark's DataFrame API**, as it aligns well with SQL-like transformations.

**Key Steps and Translations:**

1.  **SparkSession Setup**: The entry point for Spark applications.
2.  **Schema Definition**: Define `StructType` for target tables (`fin_core.dim_customers_scd2`).
3.  **Data Loading**: Simulate or actually load data from `fin_core.dim_customers_scd2` (historical active records) and `fin_landing.customer_updates` (today's incoming delta). In a real scenario, you'd use `spark.read.table("tableName")` or `spark.read.parquet("path")`, etc.
4.  **`WITH` Clauses (CTEs)**: Translate to intermediate Spark DataFrames.
5.  **Window Functions**: Use Spark's `Window` API for `ROW_NUMBER() OVER (...)`.
6.  **`FULL OUTER JOIN`**: Use Spark's `join` method.
7.  **`CASE WHEN`**: Use Spark's `when().otherwise()` functions.
8.  **`COALESCE`**: Use Spark's `coalesce` function.
9.  **`reflect("java.util.UUID", "randomUUID")`**: For UUID generation, we'll create a Spark UDF (User Defined Function) or use the built-in `uuid()` function available in Spark 3.1+.
10. **Date/Timestamp Constants**: Use `lit()` for literals and Java's `java.sql.Timestamp` or `java.time.LocalDateTime` for dynamic dates.
11. **`INSERT INTO`**: Use `df.write.mode(SaveMode.Append).insertInto("tableName")`.

---

Let's assume you have an existing Spark project. If not, set up a Maven or Gradle project with Spark dependencies.

**Maven `pom.xml` dependencies:**

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>3.5.0</version> <!-- Use your desired Spark version -->
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.5.0</version> <!-- Must match spark-core version -->
        <scope>provided</scope>
    </dependency>
    <!-- If using Spark < 3.1 for UUID, or need other reflection -->
    <!-- You might need an extra dependency or direct UDF for UUID depending on Spark version -->
</dependencies>
```

---

**Java Code (Spark DataFrame API)**

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.api.java.UDF1;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.apache.spark.sql.functions.*;

public class CustomerSCD2Processor {

    public static void main(String[] args) {
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("CustomerMDM_SCD2")
                .master("local[*]") // Use "local[*]" for local testing, or a YARN/Mesos master in production
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") // Optional: set warehouse dir for local Hive
                .enableHiveSupport() // Enable Hive integration for CREATE TABLE and INSERT INTO
                .getOrCreate();

        // --- Configuration / Parameters ---
        // In a real application, this would come from parameters or current_date()
        String processDateStr = "2024-01-01";
        Timestamp currentProcessDateTimestamp = Timestamp.valueOf(processDateStr + " 00:00:00");
        Timestamp effectiveEndDateMax = Timestamp.valueOf("9999-12-31 23:59:59");

        // --- 1. Define Target Table Schema (if not already existing in Hive Metastore) ---
        // This CREATE TABLE statement would typically be run as DDL separately or exist
        // within the Hive Metastore. Spark will pick it up.
        // For demonstration, let's ensure it's there.
        spark.sql("CREATE DATABASE IF NOT EXISTS fin_core");
        spark.sql("CREATE TABLE IF NOT EXISTS fin_core.dim_customers_scd2 (" +
                "    customer_surrogate_key STRING," +
                "    customer_id STRING," +
                "    first_name STRING," +
                "    last_name STRING," +
                "    email_address STRING," +
                "    phone_number STRING," +
                "    residential_address STRING," +
                "    marital_status STRING," +
                "    effective_start_date TIMESTAMP," +
                "    effective_end_date TIMESTAMP," +
                "    is_active BOOLEAN" +
                ") STORED AS ORC");

        // For demonstration, let's create a landing table and populate some dummy data
        spark.sql("CREATE DATABASE IF NOT EXISTS fin_landing");
        spark.sql("CREATE TABLE IF NOT EXISTS fin_landing.customer_updates (" +
                "    customer_id STRING," +
                "    first_name STRING," +
                "    last_name STRING," +
                "    email_address STRING," +
                "    phone_number STRING," +
                "    residential_address STRING," +
                "    marital_status STRING," +
                "    timestamp TIMESTAMP" +
                ") STORED AS ORC");

        // --- Register UDF for UUID generation (mimicking Hive's reflect UDF) ---
        // This is necessary if Spark version is < 3.1 where uuid() function is not built-in,
        // or if you want custom UUID generation.
        spark.udf().register("generate_uuid", (UDF1<Void, String>) arg -> UUID.randomUUID().toString(), DataTypes.StringType);
        // If Spark 3.1+ is used, you can just use functions.uuid() directly for new records.


        // --- Create dummy data for demonstration (replace with actual reads in production) ---
        // Dummy historical data (fin_core.dim_customers_scd2)
        // Assume existing active records before today's run
        spark.sql("INSERT OVERWRITE TABLE fin_core.dim_customers_scd2 VALUES " +
                "('sk1', 'cust1', 'John', 'Doe', 'john.doe@example.com', '111-222-3333', '123 Main St', 'Married', '2023-01-01 00:00:00', '9999-12-31 23:59:59', true), " +
                "('sk2', 'cust2', 'Jane', 'Smith', 'jane.smith@example.com', '444-555-6666', '456 Oak Ave', 'Single', '2023-03-15 00:00:00', '9999-12-31 23:59:59', true), " +
                "('sk3_old', 'cust3', 'Peter', 'Jones', 'peter.jones@example.com', '777-888-9999', '789 Pine Ln', 'Divorced', '2023-06-01 00:00:00', '2023-12-31 23:59:59', false)"
        );

        // Dummy incoming update data for '2024-01-01'
        spark.sql("INSERT OVERWRITE TABLE fin_landing.customer_updates VALUES " +
                // Existing customer with an update (cust1 - change email & marital status)
                "('cust1', 'John', 'Doe', 'john.doe.new@example.com', '111-222-3333', '123 Main St', 'Single', '2024-01-01 10:00:00'), " +
                // New customer (cust4)
                "('cust4', 'Alice', 'Brown', 'alice.brown@example.com', '000-111-2222', '100 River Rd', 'Married', '2024-01-01 11:00:00'), " +
                // Existing customer with no change (cust2) - should result in 'NO CHANGE'
                "('cust2', 'Jane', 'Smith', 'jane.smith@example.com', '444-555-6666', '456 Oak Ave', 'Single', '2024-01-01 12:00:00'), " +
                // Duplicate incoming update for cust1 (only the latest timestamp should be picked by row_number)
                "('cust1', 'John', 'Doe', 'john.doe.older@example.com', '111-222-3333', '123 Main St', 'Married', '2024-01-01 09:00:00'), " +
                // An update for a prior date (should be filtered out by `to_date(timestamp) = processDateStr`)
                "('cust5', 'Frank', 'Green', 'frank.green@example.com', '555-666-7777', '999 High St', 'Married', '2023-12-31 08:00:00')"
        );

        // --- Load existing dimension table and incoming updates ---
        Dataset<Row> dimCustomersSCD2 = spark.table("fin_core.dim_customers_scd2");
        Dataset<Row> customerUpdates = spark.table("fin_landing.customer_updates");

        // ----------------------------------------------------------------------------------
        // --- Translated HQL Logic: `vw_scd_transform_stg` view logic ---
        // ----------------------------------------------------------------------------------

        // WITH active_records AS (...)
        Dataset<Row> activeRecords = dimCustomersSCD2.filter(col("is_active").equalTo(true));
        activeRecords.createOrReplaceTempView("active_records"); // Optional: for debugging with spark.sql

        // WITH incoming_updates AS (...)
        WindowSpec windowSpec = Window.partitionBy("customer_id").orderBy(col("timestamp").desc());
        Dataset<Row> incomingUpdates = customerUpdates
                .filter(to_date(col("timestamp")).equalTo(lit(processDateStr))) // Filter for today's updates
                .withColumn("rn", row_number().over(windowSpec))
                .filter(col("rn").equalTo(1)) // Deduplicate
                .select(
                        col("customer_id"),
                        col("first_name"),
                        col("last_name"),
                        col("email_address"),
                        col("phone_number"),
                        col("residential_address"),
                        col("marital_status"),
                        col("timestamp").as("update_ts") // Renamed for clarity in join
                );
        incomingUpdates.createOrReplaceTempView("incoming_updates"); // Optional: for debugging with spark.sql


        // FULL OUTER JOIN handles Inserts, Updates, and Unchanged
        Dataset<Row> scdTransformStg = incomingUpdates.as("i")
                .join(activeRecords.as("a"), col("i.customer_id").equalTo(col("a.customer_id")), "fullouter")
                .select(
                        // Common customer_id
                        coalesce(col("i.customer_id"), col("a.customer_id")).as("customer_id"),

                        // Case 1: Brand new customer (Insert) OR Updated Customer (Insert new active row)
                        when(col("i.customer_id").isNotNull(), col("i.first_name")).otherwise(col("a.first_name")).as("first_name_new"),
                        when(col("i.customer_id").isNotNull(), col("i.last_name")).otherwise(col("a.last_name")).as("last_name_new"),
                        when(col("i.customer_id").isNotNull(), col("i.email_address")).otherwise(col("a.email_address")).as("email_address_new"),
                        when(col("i.customer_id").isNotNull(), col("i.phone_number")).otherwise(col("a.phone_number")).as("phone_number_new"),
                        when(col("i.customer_id").isNotNull(), col("i.residential_address")).otherwise(col("a.residential_address")).as("residential_address_new"),
                        when(col("i.customer_id").isNotNull(), col("i.marital_status")).otherwise(col("a.marital_status")).as("marital_status_new"),

                        // Evaluate if a change actually occurred
                        when(col("a.customer_id").isNull(), lit("INSERT")) // Brand New (no match in active_records)
                        .when(col("i.customer_id").isNotNull().and( // Incoming update exists AND
                                not(coalesce(col("i.last_name"), lit("")).equalTo(coalesce(col("a.last_name"), lit(""))))
                                .or(not(coalesce(col("i.email_address"), lit("")).equalTo(coalesce(col("a.email_address"), lit("")))))
                                .or(not(coalesce(col("i.residential_address"), lit("")).equalTo(coalesce(col("a.residential_address"), lit("")))))
                                .or(not(coalesce(col("i.marital_status"), lit("")).equalTo(coalesce(col("a.marital_status"), lit("")))))
                        ), lit("UPDATE")) // Update detected
                        .otherwise(lit("NO CHANGE")).as("change_type"),

                        // Retain old record details to age it out
                        col("a.customer_surrogate_key").as("old_sk"),
                        col("a.first_name").as("first_name_old"),
                        col("a.last_name").as("last_name_old"),
                        col("a.email_address").as("email_address_old"),
                        col("a.phone_number").as("phone_number_old"),
                        col("a.residential_address").as("residential_address_old"),
                        col("a.marital_status").as("marital_status_old"),
                        col("a.effective_start_date").as("old_start_date")
                );

        scdTransformStg.createOrReplaceTempView("vw_scd_transform_stg"); // Optional: for debugging with spark.sql
        System.out.println("--- vw_scd_transform_stg ---");
        scdTransformStg.show();


        // ----------------------------------------------------------------------------------
        // --- Translated HQL Logic: INSERT statements ---
        // ----------------------------------------------------------------------------------

        // 1. Insert the retired rows (closing the effective_end_date and setting is_active = false)
        // These are records that were active and are now updated, so we 'close' their previous version.
        Dataset<Row> retiredRecords = scdTransformStg
                .filter(col("change_type").equalTo("UPDATE"))
                .select(
                        col("old_sk").as("customer_surrogate_key"),
                        col("customer_id"),
                        col("first_name_old").as("first_name"),
                        col("last_name_old").as("last_name"),
                        col("email_address_old").as("email_address"),
                        col("phone_number_old").as("phone_number"),
                        col("residential_address_old").as("residential_address"),
                        col("marital_status_old").as("marital_status"),
                        col("old_start_date").as("effective_start_date"),
                        lit(currentProcessDateTimestamp).cast(DataTypes.TimestampType).as("effective_end_date"),
                        lit(false).as("is_active")
                );

        System.out.println("--- Retired Records (UPDATE change_type) ---");
        retiredRecords.show();
        retiredRecords.write().mode(SaveMode.Append).insertInto("fin_core.dim_customers_scd2");


        // 2. Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
        Dataset<Row> newActiveRecords = scdTransformStg
                .filter(col("change_type").isin("INSERT", "UPDATE"))
                .select(
                        callUDF("generate_uuid").as("customer_surrogate_key"), // Using custom UDF
                        // For Spark 3.1+ you can directly use functions.uuid()
                        // uuid().as("customer_surrogate_key"),
                        col("customer_id"),
                        col("first_name_new").as("first_name"),
                        col("last_name_new").as("last_name"),
                        col("email_address_new").as("email_address"),
                        col("phone_number_new").as("phone_number"),
                        col("residential_address_new").as("residential_address"),
                        col("marital_status_new").as("marital_status"),
                        lit(currentProcessDateTimestamp).cast(DataTypes.TimestampType).as("effective_start_date"),
                        lit(effectiveEndDateMax).cast(DataTypes.TimestampType).as("effective_end_date"),
                        lit(true).as("is_active")
                );

        System.out.println("--- New / Updated Active Records (INSERT/UPDATE change_type) ---");
        newActiveRecords.show();
        newActiveRecords.write().mode(SaveMode.Append).insertInto("fin_core.dim_customers_scd2");

        // Note: 'NO CHANGE' records are implicitly handled because `INSERT INTO` is used.
        // The original 'NO CHANGE' records from dim_customers_scd2 remain as they are,
        // and we only APPEND the changed records.

        System.out.println("--- Final fin_core.dim_customers_scd2 after SCD2 Merge ---");
        spark.table("fin_core.dim_customers_scd2").show();

        // Stop Spark Session
        spark.stop();
    }
}
```

---

**Explanation of Java (Spark) Translation:**

1.  **SparkSession**: The `spark` object is your main interface to Spark functionality.
2.  **`spark.sql("CREATE TABLE ...")`**: Used to define table schemas if they don't exist in the Hive Metastore. This is crucial for `spark.table()` and `insertInto()` to work. For a true production setup, these DDLs are often managed outside the Spark application.
3.  **UDF Registration (`generate_uuid`)**: Hive's `reflect` function is used for dynamic class calls. For UUID, we create a simple Spark UDF that calls `java.util.UUID.randomUUID().toString()`. If you are on Spark 3.1 or newer, you can directly use `functions.uuid()`.
4.  **`Dataset<Row>`**: This is Spark's core abstraction for structured data, equivalent to a table or view in SQL.
5.  **`activeRecords`**: Directly translates the `WHERE is_active = true` filter.
6.  **`incomingUpdates`**:
    *   `filter(to_date(col("timestamp")).equalTo(lit(processDateStr)))`: This replicates the HQL's `WHERE to_date(timestamp) = '2024-01-01'` to only process updates for the current processing day.
    *   `WindowSpec windowSpec = Window.partitionBy("customer_id").orderBy(col("timestamp").desc());`: Defines the window for deduplication.
    *   `withColumn("rn", row_number().over(windowSpec)).filter(col("rn").equalTo(1))`: Applies the window function and filters for the latest record per `customer_id`.
7.  **`scdTransformStg`**: This is the core `FULL OUTER JOIN` logic.
    *   `incomingUpdates.as("i").join(activeRecords.as("a"), col("i.customer_id").equalTo(col("a.customer_id")), "fullouter")`: Performs the full outer join. Aliases `i` and `a` are used for column disambiguation, just like in HQL.
    *   **`select(...)` with `when().otherwise()` and `coalesce()`**: This is how Spark DataFrame API implements `CASE WHEN` and `COALESCE` statements.
        *   `when(col("a.customer_id").isNull(), lit("INSERT"))`: Handles new records.
        *   `.when(col("i.customer_id").isNotNull().and(...)` then `lit("UPDATE")`: Identifies updates by comparing current values from `i` with old values from `a` in a null-safe way (using `coalesce` with empty string for string comparison).
        *   `.otherwise(lit("NO CHANGE"))`: Catches everything else.
8.  **Insert Statements (`retiredRecords`, `newActiveRecords`)**:
    *   Each section filters the `scdTransformStg` based on `change_type`.
    *   `select(...)`: Prepares the data with the correct columns and values for insertion into the `dim_customers_scd2` table.
        *   `lit(currentProcessDateTimestamp)` and `lit(effectiveEndDateMax)`: Use literal values for the dates.
        *   `callUDF("generate_uuid")`: Invokes our registered UUID UDF.
    *   `df.write().mode(SaveMode.Append).insertInto("fin_core.dim_customers_scd2")`: Appends the new rows to the target table. Spark handles the schema matching based on column names and types.

This Spark Java program provides a robust and scalable translation of your HQL SCD Type 2 logic. Remember to configure Spark properly for your cluster environment in a production setting (e.g., master URL, resource allocation).