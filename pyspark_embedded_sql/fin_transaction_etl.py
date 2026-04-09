Okay, this is a substantial conversion! We're moving from a Spark/Hive/Python ecosystem to a C#/BigQuery ecosystem. The core logic will shift from executing Spark SQL queries against Hive tables to executing standard SQL queries against BigQuery tables using the Google Cloud BigQuery C# client library.

Here are the key changes and considerations:

1.  **Spark Session:** This concept doesn't directly exist in BigQuery. The C# code will use the `Google.Cloud.BigQuery.V2` client to send SQL queries directly to BigQuery.
2.  **Hive Metastore/HDFS:** BigQuery serves as both the data storage and the metadata store. Tables are referenced directly by `project.dataset.table_name`.
3.  **Spark SQL Dialect:** We'll convert the Hive SQL syntax to BigQuery's Standard SQL dialect.
    *   `SET hiveconf:process_date` will become a query parameter (`@process_date`).
    *   `to_date(transaction_timestamp)` becomes `DATE(transaction_timestamp)` or `PARSE_DATE('%Y-%m-%d', transaction_timestamp)` depending on `transaction_timestamp` type.
    *   `PARTITION (col1, col2)`: BigQuery has specific partitioning (e.g., by `DATE` column, or integer range) and clustering. Overwriting partitions is typically done by `DELETE` followed by `INSERT INTO`, or by using a `MERGE` statement, or `CREATE OR REPLACE TABLE AS SELECT`. For a daily run overwriting a specific day's data, `DELETE WHERE trx_date = @process_date` and then `INSERT` is a common and straightforward pattern.
    *   `TEMPORARY VIEW`: These are often best converted to Common Table Expressions (CTEs - `WITH ... AS (...) SELECT ...`).
    *   `DECIMAL(18,4)`: BigQuery uses `NUMERIC` or `BIGNUMERIC` for precise decimals. `BIGNUMERIC` supports higher precision than `NUMERIC`.
4.  **Python `logging`:** Will be replaced with `Microsoft.Extensions.Logging` in C#.
5.  **Command-line Arguments:** Python `sys.argv` will become C# `string[] args` in the `Main` method.
6.  **`CREATE DATABASE IF NOT EXISTS`:** In BigQuery, databases are called "datasets". We'll use `BigQueryClient.CreateDatasetAsync` or `GetDataset` to ensure they exist.

---

### **Assumptions for BigQuery Structure:**

*   **Project ID:** You'll have a Google Cloud Project ID.
*   **Datasets:** `fin_landing`, `fin_core`, `fin_mart` will be BigQuery datasets within your project.
*   **Tables:**
    *   `fin_landing.raw_transactions`: Expected to be a table with the raw transaction data. Might be ingested from an external source or stream.
    *   `fin_core.fact_transactions`: We assume this table is *partitioned by `trx_date` (type `DATE`)* and potentially *clustered by `region_id`*. This allows efficient `DELETE` and `INSERT` operations for specific dates.
    *   `fin_core.dim_accounts`, `fin_core.dim_customers`: Lookup tables.
    *   `fin_mart.risk_daily_summary`: We assume this table is *partitioned by `summary_date` (type `DATE`)*.

---

### **C# Code with BigQuery Integration**

First, create a new C# Console Application project.
Then, add the necessary NuGet packages:

```bash
dotnet add package Google.Cloud.BigQuery.V2
dotnet add package Microsoft.Extensions.Logging
dotnet add package Microsoft.Extensions.Logging.Console
```

Here's the C# equivalent:

```csharp
using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

public class FinancialTransactionEtl
{
    private readonly ILogger<FinancialTransactionEtl> _logger;
    private readonly BigQueryClient _bigQueryClient;
    private readonly string _projectId;

    // --- Configuration Constants ---
    private const string LandingDataset = "fin_landing";
    private const string CoreDataset = "fin_core";
    private const string MartDataset = "fin_mart";
    private const string RawTransactionsTable = "raw_transactions";
    private const string FactTransactionsTable = "fact_transactions";
    private const string DimAccountsTable = "dim_accounts";
    private const string DimCustomersTable = "dim_customers";
    private const string RiskDailySummaryTable = "risk_daily_summary";
    // --- End Configuration Constants ---

    public FinancialTransactionEtl(ILogger<FinancialTransactionEtl> logger, string projectId)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _projectId = projectId ?? throw new ArgumentNullException(nameof(projectId));

        // BigQueryClient.Create automatically looks for GOOGLE_APPLICATION_CREDENTIALS
        // or uses ADC (Application Default Credentials).
        _bigQueryClient = BigQueryClient.Create(_projectId);
        _logger.LogInformation("BigQuery client initialized for project: {ProjectId}", _projectId);
    }

    public async Task InitializeBigQueryDatasets()
    {
        _logger.LogInformation("Ensuring BigQuery datasets exist...");
        await EnsureDatasetExists(LandingDataset);
        await EnsureDatasetExists(CoreDataset);
        await EnsureDatasetExists(MartDataset);
        _logger.LogInformation("BigQuery datasets initialization complete.");
    }

    private async Task EnsureDatasetExists(string datasetId)
    {
        try
        {
            var dataset = await _bigQueryClient.GetDatasetAsync(datasetId);
            _logger.LogInformation("Dataset '{DatasetId}' already exists.", datasetId);
        }
        catch (Google.GoogleApiException ex) when (ex.Error.Code == 404)
        {
            _logger.LogInformation("Dataset '{DatasetId}' does not exist. Creating...", datasetId);
            await _bigQueryClient.CreateDatasetAsync(datasetId);
            _logger.LogInformation("Dataset '{DatasetId}' created successfully.", datasetId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking/creating dataset '{DatasetId}'.", datasetId);
            throw; // Re-throw to indicate a critical setup failure
        }
    }

    public async Task RunEtlPipeline(string processDateString)
    {
        _logger.LogInformation("Starting ETL pipeline for process_date: {ProcessDate}", processDateString);

        // Parse the process date for type-safe parameter handling
        if (!DateTime.TryParseExact(processDateString, "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.None, out DateTime processDate))
        {
            _logger.LogError("Invalid process date format. Expected YYYY-MM-DD. Got: {ProcessDateString}", processDateString);
            throw new ArgumentException("Invalid process date format.");
        }
        
        // Use BigQueryParameter for safety and type correctness
        var dateParameter = new BigQueryParameter("process_date", BigQueryDbType.Date) { Value = processDate };


        // 1. Convert raw transactions to a CTE (Common Table Expression) for reuse within the next query
        //    BigQuery doesn't need a separate TEMPORARY VIEW creation step if used within one job.
        //    The actual select will happen within the fact_transactions insert.
        _logger.LogInformation("Defining raw transactions query for '{Date}'...", processDateString);

        // 2. Complex ETL to fact table using embedded SQL
        //    BigQuery pattern for 'INSERT OVERWRITE PARTITION': DELETE old data for the partition, then INSERT new data.
        _logger.LogInformation("Inserting data into {CoreDataset}.{FactTransactionsTable} for date {ProcessDate}...", CoreDataset, FactTransactionsTable, processDateString);

        // Delete existing data for the process date's partition
        string deleteSql = $@"
            DELETE FROM `{_projectId}.{CoreDataset}.{FactTransactionsTable}`
            WHERE trx_date = @process_date
        ";
        _logger.LogInformation("Executing DELETE for date {ProcessDate}.", processDateString);
        await _bigQueryClient.ExecuteQueryAsync(deleteSql, new[] { dateParameter });
        _logger.LogInformation("DELETE complete for date {ProcessDate}.", processDateString);

        // Insert new data
        string insertFactSql = $@"
            INSERT INTO `{_projectId}.{CoreDataset}.{FactTransactionsTable}` (
                trx_uuid,
                source_account_id,
                destination_account_id,
                transaction_type,
                amount_base_currency,
                normalized_usd_amount,
                currency_code,
                transaction_timestamp,
                merchant_category_code,
                channel,
                customer_id,
                customer_segment,
                kyc_status,
                rolling_10_trx_amount,
                status,
                trx_date,
                region_id
            )
            WITH raw_trx_delta AS (
                SELECT
                    trx_uuid,
                    source_account_id,
                    destination_account_id,
                    transaction_type,
                    amount_base_currency,
                    currency_code,
                    exchange_rate,
                    transaction_timestamp,
                    merchant_category_code,
                    channel,
                    status,
                    error_code
                FROM `{_projectId}.{LandingDataset}.{RawTransactionsTable}`
                WHERE DATE(transaction_timestamp) = @process_date
            )
            SELECT
                r.trx_uuid,
                r.source_account_id,
                r.destination_account_id,
                r.transaction_type,
                r.amount_base_currency,
                CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC(38, 4)) AS normalized_usd_amount,
                r.currency_code,
                r.transaction_timestamp,
                r.merchant_category_code,
                r.channel,
                c.customer_id,
                c.customer_segment,
                c.kyc_status,
                SUM(r.amount_base_currency) OVER (
                    PARTITION BY r.source_account_id
                    ORDER BY r.transaction_timestamp
                    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                ) AS rolling_10_trx_amount,
                r.status,
                -- Partitioning Columns
                DATE(r.transaction_timestamp) AS trx_date,
                COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
            FROM raw_trx_delta r
            LEFT JOIN `{_projectId}.{CoreDataset}.{DimAccountsTable}` dim_a ON r.source_account_id = dim_a.account_id
            LEFT JOIN `{_projectId}.{CoreDataset}.{DimCustomersTable}` c ON dim_a.customer_id = c.customer_id
            WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
              AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
        ";
        _logger.LogInformation("Executing INSERT for fact_transactions for date {ProcessDate}.", processDateString);
        await _bigQueryClient.ExecuteQueryAsync(insertFactSql, new[] { dateParameter });
        _logger.LogInformation("INSERT complete for fact_transactions for date {ProcessDate}.", processDateString);


        // 3. Create Aggregated Datamart for Risk Analysis
        _logger.LogInformation("Executing aggregation for {MartDataset}.{RiskDailySummaryTable} for date {ProcessDate}...", MartDataset, RiskDailySummaryTable, processDateString);

        // Delete existing data for the process date's partition
        string deleteRiskSql = $@"
            DELETE FROM `{_projectId}.{MartDataset}.{RiskDailySummaryTable}`
            WHERE summary_date = @process_date
        ";
        _logger.LogInformation("Executing DELETE for risk_daily_summary for date {ProcessDate}.", processDateString);
        await _bigQueryClient.ExecuteQueryAsync(deleteRiskSql, new[] { dateParameter });
        _logger.LogInformation("DELETE complete for risk_daily_summary for date {ProcessDate}.", processDateString);

        string insertRiskSql = $@"
            INSERT INTO `{_projectId}.{MartDataset}.{RiskDailySummaryTable}` (
                customer_id,
                customer_segment,
                region_id,
                total_daily_transactions,
                total_daily_volume_usd,
                max_single_transaction_usd,
                high_risk_mcc_count,
                unique_destinations_count,
                daily_risk_flag,
                summary_date
            )
            SELECT
                customer_id,
                customer_segment,
                region_id,
                COUNT(trx_uuid) AS total_daily_transactions,
                SUM(normalized_usd_amount) AS total_daily_volume_usd,
                MAX(normalized_usd_amount) AS max_single_transaction_usd,
                COUNT(CASE WHEN merchant_category_code IN ('7995', '6012') THEN 1 END) AS high_risk_mcc_count,
                COUNT(DISTINCT destination_account_id) AS unique_destinations_count,
                CASE
                    WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                    WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS daily_risk_flag,
                @process_date AS summary_date
            FROM `{_projectId}.{CoreDataset}.{FactTransactionsTable}`
            WHERE trx_date = @process_date
            GROUP BY
                customer_id,
                customer_segment,
                region_id
        ";
        _logger.LogInformation("Executing INSERT for risk_daily_summary for date {ProcessDate}.", processDateString);
        await _bigQueryClient.ExecuteQueryAsync(insertRiskSql, new[] { dateParameter });
        _logger.LogInformation("INSERT complete for risk_daily_summary for date {ProcessDate}.", processDateString);

        _logger.LogInformation("Successfully completed ETL pipeline for date {ProcessDate}.", processDateString);
    }

    public static async Task Main(string[] args)
    {
        // Setup logging
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddConsole()
                .SetMinimumLevel(LogLevel.Information);
        });
        ILogger<FinancialTransactionEtl> logger = loggerFactory.CreateLogger<FinancialTransactionEtl>();

        if (args.Length != 2)
        {
            logger.LogError("Usage: dotnet run <Project ID> <YYYY-MM-DD>");
            Environment.Exit(1);
        }

        string projectId = args[0];
        string processDate = args[1];

        try
        {
            FinancialTransactionEtl etl = new FinancialTransactionEtl(logger, projectId);
            await etl.InitializeBigQueryDatasets(); // Ensure datasets exist
            await etl.RunEtlPipeline(processDate);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An unhandled error occurred during ETL pipeline execution.");
            Environment.Exit(1);
        }
    }
}
```

---

### **How to Run (from your terminal)**

1.  **Google Cloud Authentication:**
    *   Make sure you have `gcloud CLI` installed and configured.
    *   Authenticate: `gcloud auth application-default login`
    *   This will set up `GOOGLE_APPLICATION_CREDENTIALS` environment variable automatically, or place credentials in a default location, allowing `Google.Cloud.BigQuery.V2` to find them.
    *   Alternatively, you can manually set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your service account JSON key file.

2.  **Compile and Run:**
    *   Navigate to your C# project directory in the terminal.
    *   Run:
        ```bash
        dotnet run -- <YOUR_GOOGLE_CLOUD_PROJECT_ID> 2023-10-26
        ```
        (Replace `<YOUR_GOOGLE_CLOUD_PROJECT_ID>` with your actual GCP project ID and `2023-10-26` with your desired process date).

### **Before Running:**

1.  **BigQuery Datasets:** The code attempts to create datasets if they don't exist, but it's good practice to ensure your Google Cloud Project is set up and billing is enabled.
2.  **Source Tables:** You need to have the following tables existing in your BigQuery project within the respective datasets, populated with data for testing:
    *   `your_project_id.fin_landing.raw_transactions`
    *   `your_project_id.fin_core.dim_accounts`
    *   `your_project_id.fin_core.dim_customers`
    *   Make sure `fin_core.fact_transactions` and `fin_mart.risk_daily_summary` are created with the correct schemas and are *partitioned by their respective date columns (`trx_date` and `summary_date`)* for the `DELETE` + `INSERT` strategy to be efficient.

    **Example DDL for partitioned tables (run these in BigQuery console):**

    ```sql
    -- fin_core.fact_transactions
    CREATE TABLE fin_core.fact_transactions (
        trx_uuid STRING,
        source_account_id STRING,
        destination_account_id STRING,
        transaction_type STRING,
        amount_base_currency NUMERIC(38,4),
        normalized_usd_amount BIGNUMERIC(38,4),
        currency_code STRING,
        transaction_timestamp TIMESTAMP,
        merchant_category_code STRING,
        channel STRING,
        customer_id STRING,
        customer_segment STRING,
        kyc_status STRING,
        rolling_10_trx_amount BIGNUMERIC(38,4), -- Matches normalized_usd_amount precision for SUM
        status STRING,
        trx_date DATE,
        region_id STRING
    )
    PARTITION BY trx_date
    CLUSTER BY region_id; -- Optional clustering for performance

    -- fin_mart.risk_daily_summary
    CREATE TABLE fin_mart.risk_daily_summary (
        customer_id STRING,
        customer_segment STRING,
        region_id STRING,
        total_daily_transactions INT64,
        total_daily_volume_usd BIGNUMERIC(38,4),
        max_single_transaction_usd BIGNUMERIC(38,4),
        high_risk_mcc_count INT64,
        unique_destinations_count INT64,
        daily_risk_flag STRING,
        summary_date DATE
    )
    PARTITION BY summary_date;
    ```

This conversion provides a robust C# application that interacts with BigQuery to perform the described ETL pipeline, adhering to BigQuery's best practices for data manipulation and querying.