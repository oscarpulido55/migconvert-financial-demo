Okay, this is a substantial conversion because it involves not just language (Python to C#) but also fundamental changes in the data processing paradigm (PySpark on HDFS/Hive to .NET for Apache Spark on GCS/BigQuery).

The core transformation logic can be largely maintained using **.NET for Apache Spark**. The biggest change will be:
1.  **HDFS paths** replaced by **Google Cloud Storage (GCS) paths**.
2.  **Hive table output** replaced by **BigQuery table output** using the Spark BigQuery connector.
3.  The C# code will need to be compiled and run in a Spark environment that has the Google Cloud Storage connector and the BigQuery connector installed (e.g., Dataproc).

---

### Prerequisites for Running the C# Code:

1.  **.NET SDK:** Install the .NET SDK (e.g., .NET 6 or higher).
2.  **Apache Spark:** Have an Apache Spark installation (local for testing, or Dataproc/AWS EMR/Azure HDInsight for production).
3.  **Spark BigQuery Connector:** Ensure the Spark environment has the BigQuery connector JAR. When submitting jobs to Dataproc, you'd typically specify this. For local testing, you might need to manually add it to `spark/jars`.
    *   Example `spark.jars.packages` for Spark 3.x and BigQuery: `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0` (check for the latest version).
4.  **Google Cloud Project & Service Account:**
    *   You'll need a GCP project ID.
    *   A GCS bucket for temporary files (required by the BigQuery connector).
    *   Proper authentication set up for Spark to access GCS and BigQuery (e.g., via `GOOGLE_APPLICATION_CREDENTIALS`, Dataproc service account, or passing credentials in Spark config).

---

### Python to C# Conversion (.NET for Apache Spark with BigQuery)

```csharp
// financial_customer_onboarding_etl.cs
using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql.Types; // Not explicitly used for input schemas here but good to include
using static Microsoft.Spark.Sql.Functions; // Enables direct use of functions like Col, Lit, etc.

namespace FinancialETL
{
    public class CustomerOnboarding
    {
        /// <summary>
        /// Initializes and returns a Spark session configured for BigQuery integration.
        /// </summary>
        /// <returns>A SparkSession instance.</returns>
        public static SparkSession GetSparkSession(string appName, string gcpProjectId, string gcsTempBucket)
        {
            return SparkSession.Builder()
                .AppName(appName)
                // Use default Hive support configurations if needed, but not directly for BigQuery output
                // .EnableHiveSupport() // Not directly needed when outputting to BigQuery
                .Config("spark.sql.sources.partitionOverwriteMode", "dynamic") // Still relevant for Spark's internal handling
                .Config("spark.sql.adaptive.enabled", "true")
                .Config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                // BigQuery specific configurations
                // This property usually points to the BigQuery connector jar or package coordinates.
                // In Dataproc, the connector is usually pre-installed or specified in cluster creation.
                // For local: .Config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0")
                .Config("spark.cloud.google.project.id", gcpProjectId)
                .Config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                // It's often better to configure BigQuery properties directly on the DataFrameWriter,
                // but these are common global configurations.
                .Config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .Config("fs.gs.oauth.client.id", "<YOUR_GCP_CLIENT_ID>") // Only if using client ID for auth
                .Config("fs.gs.oauth.client.secret", "<YOUR_GCP_CLIENT_SECRET>") // Only if using client secret for auth
                // Or better, let Dataproc handle service account authentication
                .GetOrCreate();
        }

        /// <summary>
        /// Main ETL process for customer onboarding.
        /// Reads raw customer data, KYC records, and initial funding details,
        /// performs complex transformations, and writes to a BigQuery dimensions table.
        /// </summary>
        /// <param name="spark">The SparkSession instance.</param>
        /// <param name="gcpProjectId">Your Google Cloud Project ID.</param>
        /// <param name="gcsLandingZonePath">Base GCS path for landing zone data.</param>
        /// <param name="bigQueryDatasetName">BigQuery dataset name.</param>
        /// <param name="bigQueryTableName">BigQuery table name.</param>
        /// <param name="gcsTempBucket">GCS bucket for BigQuery connector's temporary files.</param>
        public static void ProcessCustomerOnboarding(
            SparkSession spark,
            string gcpProjectId,
            string gcsLandingZonePath,
            string bigQueryDatasetName,
            string bigQueryTableName,
            string gcsTempBucket)
        {
            Console.WriteLine("Starting customer onboarding ETL process...");

            // --- Configuration for GCS Paths (replace with your actual paths) ---
            // Example: gs://your-landing-bucket/fin/customers/
            string rawCustomersPath = $"{gcsLandingZonePath}/customers/";
            string rawKycPath = $"{gcsLandingZonePath}/kyc_status/";
            string rawAccountsPath = $"{gcsLandingZonePath}/accounts/";

            // --- Configuration for BigQuery ---
            string bigQueryOutputTable = $"{gcpProjectId}:{bigQueryDatasetName}.{bigQueryTableName}";


            // 1. Read Raw Data sources (Simulated paths in GCS)
            Console.WriteLine($"Reading raw customer data from {rawCustomersPath}...");
            DataFrame rawCustomersDf = spark.Read().Json(rawCustomersPath);

            Console.WriteLine($"Reading raw KYC data from {rawKycPath}...");
            DataFrame rawKycDf = spark.Read().Parquet(rawKycPath);

            Console.WriteLine($"Reading raw accounts data from {rawAccountsPath}...");
            // For CSV, inferSchema can be less reliable than explicit schema definitions.
            // Consider defining a StructType for production.
            DataFrame rawAccountsDf = spark.Read().Option("header", "true").Option("inferSchema", "true").Csv(rawAccountsPath);

            // 2. Extract latest KYC status using Window Functions
            Console.WriteLine("Extracting latest KYC status...");
            WindowSpec kycWindow = Window.PartitionBy("customer_id").OrderBy(Col("verification_date").Desc());
            
            // To emulate PySpark's max.over() then filter by original column logic:
            DataFrame latestKycDf = rawKycDf
                .WithColumn("max_verification_date", Max(Col("verification_date")).Over(kycWindow))
                .Filter(Col("verification_date").EqualTo(Col("max_verification_date")))
                .Drop("max_verification_date") // Clean up the temporary column
                .WithColumnRenamed("status", "kyc_status")
                .WithColumnRenamed("risk_rating", "kyc_risk_rating");

            // 3. Aggregate Initial Funding
            Console.WriteLine("Aggregating initial funding...");
            DataFrame fundingAggDf = rawAccountsDf
                .Filter(Col("account_status").EqualTo("ACTIVE"))
                .GroupBy("customer_id")
                .Agg(
                    Sum("initial_deposit").Alias("total_initial_deposit"),
                    Max("open_date").Alias("last_account_open_date")
                );

            // 4. Join and Enrich
            Console.WriteLine("Joining and enriching customer data...");
            DataFrame enrichedCustomerDf = rawCustomersDf.Alias("c")
                .Join(Broadcast(latestKycDf).Alias("k"), Col("c.customer_id").EqualTo(Col("k.customer_id")), "left_outer")
                .Join(fundingAggDf.Alias("f"), Col("c.customer_id").EqualTo(Col("f.customer_id")), "left_outer");

            // 5. Complex Transformations: Calculate customer segments and risk profiles
            Console.WriteLine("Applying complex transformations and calculating segments...");
            DataFrame finalDimCustomers = enrichedCustomerDf.Select(
                Col("c.customer_id"),
                Col("c.first_name"),
                Col("c.last_name"),
                Col("c.ssn_hash").Alias("national_id_hash"),
                Col("c.dob").Alias("date_of_birth"),
                Col("c.address.country").Alias("country_code"),
                Col("c.address.state").Alias("state_code"),
                Coalesce(Col("k.kyc_status"), Lit("PENDING")).Alias("kyc_status"),
                Coalesce(Col("k.kyc_risk_rating"), Lit("UNKNOWN")).Alias("risk_rating"),
                Coalesce(Col("f.total_initial_deposit"), Lit(0.0)).Alias("total_initial_deposit"),
                Col("f.last_account_open_date"),
                CurrentTimestamp().Alias("etl_insert_ts")
            ).WithColumn(
                "customer_segment",
                When(Col("total_initial_deposit").Gt(1000000), Lit("PRIVATE_WEALTH"))
                .When(Col("total_initial_deposit").Between(100000, 1000000), Lit("PREMIUM"))
                .When(Col("kyc_status").EqualTo("REJECTED"), Lit("RESTRICTED"))
                .Otherwise(Lit("RETAIL"))
            ).WithColumn(
                "onboarding_year", Year(Col("last_account_open_date"))
            ).WithColumn(
                "onboarding_month", Month(Col("last_account_open_date"))
            );
            // BigQuery often prefers a DATE or TIMESTAMP column for native partitioning,
            // so adding a full date for easier BigQuery partitioning might be beneficial.
            // .WithColumn("onboarding_date", ToDate(Col("last_account_open_date")));

            // 6. Write to BigQuery Table
            Console.WriteLine($"Writing to BigQuery table: {bigQueryOutputTable}...");
            // BigQuery tables are usually partitioned natively based on a DATE/TIMESTAMP column in the schema.
            // The `partitionBy` in Spark typically means writing partitioned files to GCS.
            // When using the BigQuery connector, you configure the BigQuery specific partitioning via options.
            // Example for partition overwrite:
            // The BigQuery connector can overwrite partitions. This typically involves
            // ensuring your BigQuery table is partitioned by a field like `onboarding_date` (DATE)
            // or `etl_insert_ts` (TIMESTAMP).
            // For this `partitionBy` (onboarding_year, onboarding_month, country_code), if you want to leverage
            // BigQuery's native partitioning, you'd define the BQ table with one of these.
            // If the goal is to overwrite partitions, `WRITE_TRUNCATE` is for the whole table.
            // For dynamic partition overwrite, you usually write to a temporary table/GCS, then use DML.
            // The BQ connector offers ways to control this.
            
            // This example will simply overwrite the entire target table (WRITE_TRUNCATE mode in BQ)
            // If you want to append, use SaveMode.Append.
            // For more advanced partitioning (e.g., overwrite only specific partitions),
            // you'd need to add `option("partitionField", "onboarding_year")`
            // and `option("writeMethod", "indirect")` with DML on temp tables.
            
            finalDimCustomers.Write()
                .Format("bigquery")
                .Option("table", bigQueryOutputTable)
                .Option("project", gcpProjectId)
                .Option("temporaryGcsBucket", gcsTempBucket)
                // If you want to overwrite the whole table, use SaveMode.Overwrite
                .Mode(SaveMode.Overwrite)
                // If the BQ table is partitioned by 'onboarding_year', you could specify:
                // .Option("partitionField", "onboarding_year")
                // For partition overwrite specific to a column (needs `SaveMode.Overwrite` and may use DML under the hood)
                // .Option("partitionOverwriteMode", "dynamic") // This applies to the *Spark* side of things primarily.
                                                              // BigQuery connector specific options are better here.
                .Save();

            long recordCount = finalDimCustomers.Count();
            Console.WriteLine($"Successfully processed {recordCount} customer records.");
            Console.WriteLine("ETL process completed.");
        }

        public static void Main(string[] args)
        {
            // --- Configuration variables (replace with your actual values) ---
            string gcpProjectId = Environment.GetEnvironmentVariable("GCP_PROJECT_ID") ?? "your-gcp-project-id";
            string gcsLandingZonePath = Environment.GetEnvironmentVariable("GCS_LANDING_ZONE_PATH") ?? "gs://your-landing-bucket/fin";
            string bigQueryDatasetName = Environment.GetEnvironmentVariable("BIGQUERY_DATASET") ?? "fin_core";
            string bigQueryTableName = Environment.GetEnvironmentVariable("BIGQUERY_TABLE") ?? "dim_customers";
            string gcsTempBucket = Environment.GetEnvironmentVariable("GCS_TEMP_BUCKET") ?? "your-temp-bucket-for-spark-bq";

            SparkSession spark = null;
            try
            {
                spark = GetSparkSession("Financial_Customer_Onboarding_ETL_CS", gcpProjectId, gcsTempBucket);
                
                // BigQuery datasets must be created before running this. Spark does not create them.
                // You'd typically create them via `bq mk fin_core` or Terraform/Cloud Console.
                Console.WriteLine($"Ensure BigQuery dataset '{bigQueryDatasetName}' exists in project '{gcpProjectId}'.");

                ProcessCustomerOnboarding(
                    spark,
                    gcpProjectId,
                    gcsLandingZonePath,
                    bigQueryDatasetName,
                    bigQueryTableName,
                    gcsTempBucket);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                if (spark != null)
                {
                    spark.Stop();
                    Console.WriteLine("Spark session stopped.");
                }
            }
        }
    }
}
```

---

### Project Setup and Execution

1.  **Create a .NET Project:**
    ```bash
    dotnet new console -n FinancialETL
    cd FinancialETL
    ```

2.  **Install .NET for Apache Spark NuGet Package:**
    ```bash
    dotnet add package Microsoft.Spark
    ```

3.  **Place the C# Code:**
    Replace the contents of `Program.cs` (or create a new `CustomerOnboarding.cs` file) with the C# code provided above.

4.  **Build the Project:**
    ```bash
    dotnet build
    ```

5.  **Run with `spark-submit`:**
    You'll need to submit this as a Spark job.
    **A. For Local Testing (if you have Spark installed locally):**
    First, ensure you have the `spark-bigquery-with-dependencies_2.12:0.33.0` (or latest) JAR in your Spark's `jars` directory or specify it with `--packages`.

    ```bash
    # Set environment variables (replace with your actual values)
    export GCP_PROJECT_ID="your-gcp-project-id"
    export GCS_LANDING_ZONE_PATH="gs://your-landing-bucket/fin"
    export BIGQUERY_DATASET="fin_core"
    export BIGQUERY_TABLE="dim_customers"
    export GCS_TEMP_BUCKET="your-temp-bucket-for-spark-bq"
    
    # Example using spark-submit (adjust Spark home, .NET version as needed)
    spark-submit \
        --class org.apache.spark.deploy.dotnet.DotnetRunner \
        --master local[*] \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0 \
        --conf spark.executor.extraJavaOptions=-Dgoogle.cloud.auth.service.account.json_keyfile=$GOOGLE_APPLICATION_CREDENTIALS \
        --conf spark.driver.extraJavaOptions=-Dgoogle.cloud.auth.service.account.json_keyfile=$GOOGLE_APPLICATION_CREDENTIALS \
        --packages Microsoft.Spark \
        # Ensure the path to the worker .NET host and app is correct
        --files /path/to/spark-3.x.x-bin-hadoopx.x/bin/Microsoft.Spark.Worker/Microsoft.Spark.Worker.dll,/path/to/FinancialETL/bin/Debug/net6.0/FinancialETL.dll \
        /path/to/spark-3.x.x-bin-hadoopx.x/bin/Microsoft.Spark.Worker/Microsoft.Spark.Worker.dll \
        Microsoft.Spark.Worker.exe \
        /path/to/FinancialETL/bin/Debug/net6.0/FinancialETL.dll
    ```
    **Note on Local GCS/BigQuery Authentication:** For local `spark-submit`, you'll likely need to set `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to your service account JSON key file before running.

    **B. For Google Cloud Dataproc:**
    This is the recommended way. Dataproc clusters come pre-configured with GCS and BigQuery connectors.

    ```bash
    # Build your .NET application for production
    dotnet publish -c Release

    # Upload your compiled application to GCS
    gsutil cp bin/Release/net6.0/publish/* gs://your-code-bucket/etl-apps/financial-etl/

    # Submit job to Dataproc
    gcloud dataproc jobs submit spark \
        --cluster=your-dataproc-cluster-name \
        --region=your-gcp-region \
        --class=org.apache.spark.deploy.dotnet.DotnetRunner \
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12.jar \
        --files=gs://spark-lib/dotnet/Microsoft.Spark.Worker/Microsoft.Spark.Worker.dll \
        --conf spark.dotnet.assembly.csx=gs://your-code-bucket/etl-apps/financial-etl/FinancialETL.dll \
        --properties spark.sql.sources.partitionOverwriteMode=dynamic,spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true \
        -- arg0=Microsoft.Spark.Worker.exe arg1=gs://your-code-bucket/etl-apps/financial-etl/FinancialETL.dll \
        --labels=purpose=financial-etl \
        --project=your-gcp-project-id \
        --driver-log-levels=root=WARN \
        --max-failures-per-hour=1 \
        --env-vars=GCP_PROJECT_ID=your-gcp-project-id,GCS_LANDING_ZONE_PATH=gs://your-landing-bucket/fin,BIGQUERY_DATASET=fin_core,BIGQUERY_TABLE=dim_customers,GCS_TEMP_BUCKET=your-temp-bucket-for-spark-bq
    ```
    **Note:** Adjust `--jars` and `--files` paths based on your Dataproc version and where the .NET Worker and BigQuery connector are located/staged. `spark-lib/bigquery` and `spark-lib/dotnet/Microsoft.Spark.Worker` are common paths for pre-installed components on Dataproc.

---

### Key Changes and Considerations:

*   **File Paths:** All HDFS paths (`hdfs://namenode:8020/...`) are changed to Google Cloud Storage (GCS) paths (`gs://your-bucket/path/...`).
*   **BigQuery Output:** Instead of `saveAsTable("fin_core.dim_customers")` which targets Hive, the C# code uses `.Format("bigquery").Option("table", ...).Save()`. This leverages the Spark BigQuery connector.
*   **Partitioning:**
    *   In PySpark, `partitionBy("onboarding_year", "onboarding_month", "country_code").format("orc").mode("overwrite").saveAsTable("fin_core.dim_customers")` would create partitioned directories on HDFS/S3 and register them with Hive.
    *   For BigQuery, native table partitioning is typically on a `DATE`, `TIMESTAMP`, or `INTEGER` column within the BigQuery table schema itself.
    *   The BigQuery connector allows you to specify a `partitionField` option for writing, but `SaveMode.Overwrite` by default overwrites the *entire* table in BigQuery. If you truly want to overwrite specific partitions, you'd need a more advanced BigQuery connector configuration (`writeMethod=indirect`, specify the partition, or use DML after writing to a temporary table). I've chosen `SaveMode.Overwrite` as the most direct translation of `mode("overwrite")` for simplicity, assuming a full table refresh.
*   **Spark Session Configuration:** Added BigQuery-specific configurations like `spark.cloud.google.project.id` and temporary GCS bucket for the connector.
*   **Authentication:** Crucial for BigQuery and GCS access. Dataproc's service account usually handles this, but for local testing, `GOOGLE_APPLICATION_CREDENTIALS` is typical.
*   **`inferSchema`:** While supported, defining explicit `StructType` for input schemas in C# often leads to more robust production code.
*   **Console Logging:** `print()` calls are replaced with `Console.WriteLine()`.
*   **`main` block:** Standard C# `Main` method with `try-catch-finally` for proper Spark session management.
*   **UDFs:** If you had `udf`s in PySpark, they would need to be re-implemented as .NET for Apache Spark UDFs (which requires writing a static C# method and registering it). The provided code doesn't have custom UDFs, only built-in functions.