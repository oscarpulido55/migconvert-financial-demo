Converting the PySpark (Python) code to C# using BigQuery as the database involves a few key transformations:

1.  **Spark DataFrame API to BigQuery SQL:** This is the most significant change. PySpark DataFrames offer a programmatic way to build query plans. BigQuery prefers native SQL. We will translate the DataFrame operations (joins, filters, aggregations, window functions, `when`/`otherwise` logic) into a single, complex BigQuery SQL query, likely using Common Table Expressions (CTEs) for readability and logical separation, similar to how Spark stages are chained.
2.  **UDFs (Haversine):** The Python `haversine_udf` will be translated into a BigQuery SQL UDF or JavaScript UDF. For performance and common calculations, a SQL UDF is often preferred.
3.  **Spark Session to BigQuery Client:** Instead of a `SparkSession`, you'll use the `Google.Cloud.BigQuery.V2` .NET client library to interact with BigQuery.
4.  **Database Access:** `spark.table("fin_core.cc_transactions")` becomes `SELECT * FROM `your-gcp-project.fin_core.cc_transactions`` in BigQuery SQL.
5.  **Date Handling:** PySpark's date functions like `date_sub` and `lit` will be replaced with BigQuery SQL date functions like `PARSE_DATE`, `DATE_SUB`, `INTERVAL`.
6.  **`sys.argv`:** Standard C# `string[] args` for command-line arguments.

---

### BigQuery UDF Setup (Run this in BigQuery Console once)

First, you need to create the Haversine UDF in your BigQuery dataset. Choose a dataset (e.g., `your_project.your_dataset_for_udf`) where you want to store utility UDFs.

```sql
-- Replace `your_gcp_project_id` and `your_dataset_for_udf` with your actual project and dataset IDs
CREATE OR REPLACE FUNCTION `your_gcp_project_id.your_dataset_for_udf.haversine`(
    lat1 FLOAT64, lon1 FLOAT64, lat2 FLOAT64, lon2 FLOAT64
) RETURNS FLOAT64
AS (
    IF(lat1 IS NULL OR lon1 IS NULL OR lat2 IS NULL OR lon2 IS NULL, -1.0,
        ACOS(
            COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * COS(RADIANS(lon2) - RADIANS(lon1)) +
            SIN(RADIANS(lat1)) * SIN(RADIANS(lat2))
        ) * 6371.0
    )
);

-- Note: The Python haversine used a slightly more robust `atan2` variant.
-- This SQL UDF uses the spherical law of cosines (`ACOS`), which is generally simpler to
-- express in SQL and often sufficient. If precise numerical stability near poles/antipodes
-- is critical, a JavaScript UDF (which can use Math.atan2) would be a more direct translation.
```

---

### C# .NET Application

Here's the C# equivalent.

**1. Project Setup:**

Create a new C# console application project.

```bash
dotnet new console -n CreditCardFraudScoring
cd CreditCardFraudScoring
```

**2. Add BigQuery NuGet Package:**

```bash
dotnet add package Google.Cloud.BigQuery.V2
```

**3. `Program.cs` file:**

```csharp
using Google.Cloud.BigQuery.V2;
using System;
using System.Threading.Tasks;

public class Program
{
    // === CONFIGURATION ===
    // Replace with your actual GCP Project ID
    private const string ProjectId = "your-gcp-project-id";
    // Replace with the dataset where your `fin_core` tables (cc_transactions, dim_accounts, dim_merchants) are located
    private const string FinCoreDatasetId = "fin_core";
    // Replace with the dataset where your `fin_mart` output table (fraud_scores_daily) is located
    private const string FinMartDatasetId = "fin_mart";
    // Replace with the dataset where you created the `haversine` UDF
    private const string UdfDatasetId = "your_dataset_for_udf"; // e.g., "utils" or "common_udfs"

    // Optional: Pre-create the destination table schema in BigQuery for best practice.
    // Example DDL:
    /*
    CREATE TABLE `your-gcp-project-id.fin_mart.fraud_scores_daily` (
        trx_id STRING,
        account_id STRING,
        amount FLOAT64,
        distance_from_home_km FLOAT64,
        amount_z_score FLOAT64,
        trx_last_hour_cnt INT64,
        fraud_score INT64,
        is_fraud_alert BOOL,
        scoring_date DATE
    );
    */

    public static async Task Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Usage: dotnet run <YYYY-MM-DD>");
            Environment.Exit(1);
        }

        string executionDate = args[0]; // Format expected: YYYY-MM-DD

        try
        {
            Console.WriteLine($"Starting BigQuery fraud scoring for {executionDate}...");
            await ScoreTransactionsForFraud(executionDate);
            Console.WriteLine($"BigQuery fraud scoring completed successfully for {executionDate}");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"An error occurred: {ex.Message}");
            Console.Error.WriteLine(ex.StackTrace);
            Environment.Exit(1);
        }
    }

    private static async Task ScoreTransactionsForFraud(string executionDate)
    {
        var client = BigQueryClient.Create(ProjectId);

        // This SQL query encapsulates all the logic from the PySpark script using CTEs (Common Table Expressions)
        string query = $@"
            WITH
            -- 1. Load Data & Filter Current Day Transactions
            --    Also joins accounts and merchants to prepare for distance calculation
            CurrentDayEnrichedTransactions AS (
                SELECT
                    t.trx_id,
                    t.account_id,
                    t.amount,
                    t.trx_timestamp,
                    t.trx_date,
                    a.home_lat,
                    a.home_lon,
                    m.merchant_lat,
                    m.merchant_lon
                FROM
                    `{ProjectId}.{FinCoreDatasetId}.cc_transactions` AS t
                INNER JOIN
                    `{ProjectId}.{FinCoreDatasetId}.dim_accounts` AS a ON t.account_id = a.account_id
                LEFT JOIN
                    `{ProjectId}.{FinCoreDatasetId}.dim_merchants` AS m ON t.merchant_id = m.merchant_id
                WHERE
                    t.trx_date = PARSE_DATE('%Y-%m-%d', @executionDate)
            ),
            -- 2. Calculate Distance from Home (using the BigQuery UDF)
            TransactionsWithDistance AS (
                SELECT
                    *,
                    `{ProjectId}.{UdfDatasetId}.haversine`(home_lat, home_lon, merchant_lat, merchant_lon) AS distance_from_home_km
                FROM
                    CurrentDayEnrichedTransactions
            ),
            -- 3. Pure BigQuery Historical Profiling (Last 90 Days)
            HistoricalProfile AS (
                SELECT
                    account_id,
                    AVG(amount) AS avg_trx_amount_90d,
                    IFNULL(STDDEV_SAMP(amount), 0.0) AS stddev_trx_amount_90d,
                    COUNT(trx_id) / 90.0 AS avg_daily_trx_count
                FROM
                    `{ProjectId}.{FinCoreDatasetId}.cc_transactions`
                WHERE
                    trx_date >= DATE_SUB(PARSE_DATE('%Y-%m-%d', @executionDate), INTERVAL 90 DAY) AND
                    trx_date <= DATE_SUB(PARSE_DATE('%Y-%m-%d', @executionDate), INTERVAL 1 DAY)
                GROUP BY
                    account_id
            ),
            -- 4. Join Current Day Transactions with Historical Profiles
            FeaturesReady AS (
                SELECT
                    td.*,
                    hp.avg_trx_amount_90d,
                    hp.stddev_trx_amount_90d,
                    hp.avg_daily_trx_count
                FROM
                    TransactionsWithDistance AS td
                LEFT JOIN
                    HistoricalProfile AS hp ON td.account_id = hp.account_id
            ),
            -- 5. Apply Window Functions and Initial Score Calculations
            CalculatedScores AS (
                SELECT
                    trx_id,
                    account_id,
                    amount,
                    trx_timestamp, -- Retain for window function
                    distance_from_home_km,
                    -- Time-based Window Function (Last Hour Trx Count)
                    COUNT(trx_id) OVER (
                        PARTITION BY account_id
                        ORDER BY trx_timestamp
                        RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
                    ) AS trx_last_hour_cnt,
                    -- Amount Z-Score
                    CASE
                        WHEN stddev_trx_amount_90d > 0 THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d
                        ELSE 0.0
                    END AS amount_z_score,
                    -- Distance Risk Score
                    CASE
                        WHEN distance_from_home_km > 500 AND distance_from_home_km != -1.0 THEN 30
                        ELSE 0
                    END AS distance_risk_score,
                    -- Amount Risk Score
                    CASE
                        WHEN (CASE WHEN stddev_trx_amount_90d > 0 THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d ELSE 0.0 END) > 3.0 THEN 40
                        WHEN (CASE WHEN stddev_trx_amount_90d > 0 THEN (amount - avg_trx_amount_90d) / stddev_trx_amount_90d ELSE 0.0 END) > 2.0 THEN 20
                        ELSE 0
                    END AS amount_risk_score
                FROM
                    FeaturesReady
            ),
            -- 6. Calculate Velocity Risk Score and Final Fraud Score
            FinalScores AS (
                SELECT
                    trx_id,
                    account_id,
                    amount,
                    distance_from_home_km,
                    amount_z_score,
                    trx_last_hour_cnt,
                    distance_risk_score,
                    amount_risk_score,
                    -- Velocity Risk Score
                    CASE
                        WHEN trx_last_hour_cnt > 5 THEN 30
                        WHEN trx_last_hour_cnt > 3 THEN 15
                        ELSE 0
                    END AS velocity_risk_score
                FROM
                    CalculatedScores
            )
            -- 7. Select final columns and prepare for insert
            SELECT
                fs.trx_id,
                fs.account_id,
                fs.amount,
                fs.distance_from_home_km,
                fs.amount_z_score,
                fs.trx_last_hour_cnt,
                (fs.distance_risk_score + fs.amount_risk_score + fs.velocity_risk_score) AS fraud_score,
                (fs.distance_risk_score + fs.amount_risk_score + fs.velocity_risk_score) >= 60 AS is_fraud_alert,
                PARSE_DATE('%Y-%m-%d', @executionDate) AS scoring_date
            FROM
                FinalScores AS fs
        ";

        // Define query parameters
        var parameters = new[]
        {
            new BigQueryParameter("executionDate", BigQueryDbType.String, executionDate)
        };

        // Set up job configuration for appending to the target table
        // This mirrors Spark's `mode("append").insertInto(...)`
        var jobConfiguration = new JobConfigurationQuery
        {
            Query = query,
            DestinationTable = new TableReference
            {
                ProjectId = ProjectId,
                DatasetId = FinMartDatasetId,
                TableId = "fraud_scores_daily"
            },
            WriteDisposition = WriteDisposition.WriteAppend, // Appends new rows to the table
            UseLegacySql = false // Use Standard SQL
        };

        // Create the BigQuery job to execute the query
        Job job = await client.CreateQueryJobAsync(jobConfiguration, parameters);

        // Wait for the job to complete
        Job completedJob = await job.PollUntilCompletedAsync();

        // Check for job errors
        if (completedJob.Status.ErrorResult != null)
        {
            throw new BigQueryException(
                $"BigQuery Job failed: {completedJob.Status.ErrorResult.Message}. " +
                $"Location: {completedJob.Status.ErrorResult.Location}. " +
                $"Reason: {completedJob.Status.ErrorResult.Reason}"
            );
        }
    }
}
```

---

### How to Run:

1.  **GCP Authentication:** Ensure your environment is authenticated to GCP.
    *   **Local Development:** Use `gcloud auth application-default login` or set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account key JSON file.
    *   **GCP Environments (Cloud Run, GKE, Cloud Functions, Compute Engine):** Typically, authentication happens automatically via the attached service account.
2.  **Replace Placeholders:** Update `ProjectId`, `FinCoreDatasetId`, `FinMartDatasetId`, and `UdfDatasetId` in `Program.cs` with your actual BigQuery project and dataset IDs.
3.  **Deploy UDF:** Make sure you've run the `CREATE OR REPLACE FUNCTION ... haversine` SQL in your BigQuery console first.
4.  **Build and Run:**

    ```bash
    dotnet build
    dotnet run -- 2023-10-26
    ```
    (Replace `2023-10-26` with your desired `execution_date`).

### Key Differences and Notes:

*   **`SparkSession` vs. `BigQueryClient`**: C# directly uses the `Google.Cloud.BigQuery.V2` client to send SQL queries.
*   **`pyspark.sql.functions` vs. SQL**: All Spark functions are translated into their BigQuery SQL equivalents (e.g., `col("trx_date") == execution_date` becomes `t.trx_date = PARSE_DATE('%Y-%m-%d', @executionDate)`).
*   **Window Functions**: The `Window` object in Spark translates to `OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN ...)` in BigQuery SQL. The `unix_timestamp` based range in Spark is handled by `TIMESTAMP` intervals in BigQuery (e.g., `INTERVAL 1 HOUR PRECEDING`).
*   **UDF Naming**: In BigQuery, UDFs must be fully qualified with project and dataset names when called (e.g., ``your_gcp_project_id.your_dataset_for_udf.haversine`(...).
*   **Data Types**: Be mindful of data type conversions between Python, Spark, and BigQuery. The client library handles much of this, but SQL queries need to use correct BigQuery types (e.g., `FLOAT64`, `DATE`, `TIMESTAMP`).
*   **`fillna(0.0)`**: Translated to `IFNULL(STDDEV_SAMP(amount), 0.0)` in BigQuery SQL.
*   **`lit(execution_date)`**: In SQL, `PARSE_DATE('%Y-%m-%d', @executionDate)` serves this purpose, turning the string parameter into a BigQuery `DATE` type.
*   **Performance**: Putting all logic into a single BigQuery SQL query with CTEs is generally very performant, as BigQuery's optimizer can execute it efficiently on its distributed architecture. This is analogous to how Spark optimizes a DAG of DataFrame operations.
*   **Schema Evolution**: If `fin_mart.fraud_scores_daily` doesn't exist, BigQuery will attempt to infer the schema from the query result when `WriteDisposition.WriteAppend` is used (provided the dataset has schema auto-detection enabled). It's best practice to pre-create your destination tables with explicit schemas.