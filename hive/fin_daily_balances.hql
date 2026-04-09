The provided HQL script calculates End-Of-Day (EOD) balances by processing daily transactions and integrating them with the previous day's balances. To convert this to C# interacting with BigQuery, we'll follow these steps:

1.  **BigQuery Schema Conversion**: Translate Hive types (`DECIMAL(18,4)`, `DATE`, `STRING`, `BOOLEAN`, `TIMESTAMP`) and partitioning (`PARTITIONED BY`, `STORED AS ORC`) to BigQuery equivalents (`NUMERIC(38,4)`, `DATE`, `STRING`, `BOOL`, `TIMESTAMP`, `PARTITION BY`, `CLUSTER BY`).
2.  **SQL Query Adaptation**:
    *   Replace `CREATE DATABASE` with `BigQueryClient.CreateDatasetAsync`.
    *   Replace `CREATE TEMPORARY TABLE` with a Common Table Expression (CTE) for the `DailyMovements` logic, which is a BigQuery best practice.
    *   Translate Hive's `INSERT OVERWRITE TABLE ... PARTITION` into a BigQuery `DELETE` statement for the target partition (if it exists) followed by an `INSERT INTO` statement. This ensures idempotency and correct partition management.
    *   Adjust date functions (e.g., `DATE_SUB` syntax).
    *   Fully qualify table names with `project.dataset.table`.
3.  **C# Application Logic**:
    *   Use the `Google.Cloud.BigQuery.V2` client library.
    *   Create a class to encapsulate BigQuery operations.
    *   Implement methods for ensuring the dataset and target table exist, and for executing the daily balance calculation query.
    *   Handle dynamic dates using parameters to the SQL queries.
    *   Include error handling and console output for visibility.

---

### Prerequisites:

1.  **Google Cloud Project**: You need an active Google Cloud project with the BigQuery API enabled.
2.  **Authentication**: Your C# application needs to authenticate to Google Cloud. The recommended way for development is `gcloud auth application-default login`. For deployment, use a service account.
3.  **NuGet Package**: Install the `Google.Cloud.BigQuery.V2` package in your C# project.
    ```bash
    dotnet add package Google.Cloud.BigQuery.V2
    ```
4.  **Source Tables**: Ensure `fin_core.dim_accounts` and `fin_core.fact_transactions` tables exist in your BigQuery dataset with appropriate schemas as per the original HQL query.
    *   `fin_core.dim_accounts`: `account_id` (STRING), `customer_id` (STRING), `account_type` (STRING), `open_date` (DATE), `currency_code` (STRING), `region_code` (STRING), `status` (STRING).
    *   `fin_core.fact_transactions`: `destination_account_id` (STRING), `source_account_id` (STRING), `amount_base_currency` (NUMERIC), `trx_date` (DATE), `transaction_type` (STRING).

---

### C# Code (`Program.cs`):

```csharp
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// Main namespace for the application
namespace FinDailyBalancesBigQuery
{
    public class DailyBalanceCalculator
    {
        private readonly BigQueryClient _bigQueryClient;
        private readonly string _projectId;
        private readonly string _datasetId;

        // Constructor for the calculator, requires project and dataset IDs
        public DailyBalanceCalculator(string projectId, string datasetId)
        {
            _projectId = projectId ?? throw new ArgumentNullException(nameof(projectId));
            _datasetId = datasetId ?? throw new ArgumentNullException(nameof(datasetId));
            
            // Initialize BigQueryClient.
            // This relies on Application Default Credentials (ADC) or GOOGLE_APPLICATION_CREDENTIALS environment variable.
            // For explicit service account key file authentication, use:
            // BigQueryClient.Create(projectId, Google.Apis.Auth.OAuth2.GoogleCredential.FromFile("path/to/key.json"));
            _bigQueryClient = BigQueryClient.Create(_projectId);
        }

        // Main method to orchestrate the EOD balance calculation process
        public async Task RunDailyBalanceProcessAsync(DateTime processDate)
        {
            Console.WriteLine($"--- Starting Daily Balance Calculation for {processDate:yyyy-MM-dd} ---");

            try
            {
                // Step 1: Ensure the BigQuery dataset exists
                await EnsureDatasetExistsAsync();

                // Step 2: Ensure the target fact_daily_balances table exists with the correct schema, partitioning, and clustering
                await EnsureFactDailyBalancesTableExistsAsync();

                // Step 3: Calculate and insert/overwrite daily balances for the specified date
                await CalculateAndInsertDailyBalancesAsync(processDate);

                Console.WriteLine($"--- Daily Balance Calculation for {processDate:yyyy-MM-dd} Completed Successfully ---");
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERROR: An error occurred during daily balance calculation: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                Console.ResetColor();
            }
        }

        // Helper method to execute a BigQuery SQL query asynchronously
        private async Task ExecuteBigQuerySqlAsync(string sql, IEnumerable<BigQueryParameter> parameters = null, string jobDescription = null)
        {
            Console.WriteLine($"\nExecuting BigQuery Job: {jobDescription ?? "SQL Query"}...");
            Console.WriteLine($"SQL: \n{sql.Trim()}\n");

            var queryOptions = new QueryOptions
            {
                Parameters = parameters?.ToList() // Pass parameters to prevent SQL injection and improve caching
            };

            // Create and run the query job, then wait for its completion
            BigQueryJob job = await _bigQueryClient.CreateQueryJobAsync(sql, queryOptions);
            await job.PollUntilCompletedAsync(); // Blocks until the job finishes or fails

            if (job.Status.ErrorResult != null)
            {
                throw new BigQueryException($"BigQuery job failed: {job.Status.ErrorResult.Message}. Errors: {string.Join(", ", job.Status.Errors.Select(e => e.Message))}");
            }

            Console.WriteLine($"BigQuery Job '{jobDescription ?? "SQL Query"}' Completed. Job ID: {job.Reference.JobId}");
        }

        // Ensures the specified BigQuery dataset exists, creating it if it does not.
        private async Task EnsureDatasetExistsAsync()
        {
            Console.WriteLine($"Checking if dataset '{_datasetId}' exists in project '{_projectId}'...");
            try
            {
                // Attempt to retrieve the dataset. If it doesn't exist, an exception is thrown.
                await _bigQueryClient.GetDatasetAsync(_datasetId);
                Console.WriteLine($"Dataset '{_datasetId}' already exists.");
            }
            catch (Google.GoogleApiException ex) when (ex.Error.Code == 404) // 404 indicates resource not found
            {
                Console.WriteLine($"Dataset '{_datasetId}' not found. Creating it...");
                await _bigQueryClient.CreateDatasetAsync(_datasetId);
                Console.WriteLine($"Dataset '{_datasetId}' created successfully.");
            }
        }

        // Ensures the `fact_daily_balances` table exists with the correct BigQuery schema, partitioning, and clustering.
        private async Task EnsureFactDailyBalancesTableExistsAsync()
        {
            Console.WriteLine($"Checking if table '{_datasetId}.fact_daily_balances' exists...");

            var tableRef = _bigQueryClient.GetTableReference(_datasetId, "fact_daily_balances");
            try
            {
                // Attempt to retrieve the table. If it doesn't exist, an exception is thrown.
                await _bigQueryClient.GetTableAsync(tableRef);
                Console.WriteLine($"Table '{_datasetId}.fact_daily_balances' already exists.");
            }
            catch (Google.GoogleApiException ex) when (ex.Error.Code == 404) // 404 indicates resource not found
            {
                Console.WriteLine($"Table '{_datasetId}.fact_daily_balances' not found. Creating it...");

                // Define the BigQuery schema for the table
                var schema = new TableSchema
                {
                    Fields = new List<TableField>
                    {
                        new TableField { Name = "account_id", Type = "STRING", Mode = "REQUIRED", Description = "Unique identifier for the account" },
                        new TableField { Name = "customer_id", Type = "STRING", Mode = "REQUIRED", Description = "Identifier for the account owner" },
                        new TableField { Name = "account_type", Type = "STRING", Mode = "REQUIRED", Description = "Type of account (CHECKING, SAVINGS, LOAN)" },
                        new TableField { Name = "open_date", Type = "DATE", Mode = "REQUIRED", Description = "Date the account was opened" },
                        new TableField { Name = "currency_code", Type = "STRING", Mode = "REQUIRED", Description = "Base currency of the account" },
                        new TableField { Name = "beginning_balance", Type = "NUMERIC", Mode = "REQUIRED", Description = "Balance at the start of the day" },
                        new TableField { Name = "total_credits", Type = "NUMERIC", Mode = "REQUIRED", Description = "Total value of incoming funds" },
                        new TableField { Name = "total_debits", Type = "NUMERIC", Mode = "REQUIRED", Description = "Total value of outgoing funds" },
                        new TableField { Name = "ending_balance", Type = "NUMERIC", Mode = "REQUIRED", Description = "Balance at the end of the day" },
                        new TableField { Name = "interest_accrued", Type = "NUMERIC", Mode = "REQUIRED", Description = "Daily interest accrued" },
                        new TableField { Name = "is_overdrawn", Type = "BOOL", Mode = "REQUIRED", Description = "Flag indicating if the account is in negative balance" },
                        new TableField { Name = "etl_timestamp", Type = "TIMESTAMP", Mode = "REQUIRED", Description = "Timestamp when the ETL process ran" },
                        // These columns are part of the schema and will also serve for partitioning/clustering
                        new TableField { Name = "balance_date", Type = "DATE", Mode = "REQUIRED", Description = "The date for which the balance is calculated (Partition Key)" },
                        new TableField { Name = "region_code", Type = "STRING", Mode = "REQUIRED", Description = "Region identifier for the account (Clustering Key)" }
                    }
                };

                // Define table creation options, including partitioning and clustering
                var tableOptions = new CreateTableOptions
                {
                    TimePartitioning = TimePartitioning.CreateDailyPartitioning("balance_date"), // Partition by the 'balance_date' column, daily
                    ClusteringFields = new[] { "region_code", "account_id" }, // Cluster data within partitions by 'region_code' then 'account_id'
                    Description = "Stores the End of Day balances for all accounts"
                };

                await _bigQueryClient.CreateTableAsync(_datasetId, "fact_daily_balances", schema, tableOptions);
                Console.WriteLine($"Table '{_datasetId}.fact_daily_balances' created successfully.");
            }
        }

        // Calculates and inserts/overwrites the daily balances for the specified date.
        // This method performs two main BigQuery operations: DELETE and INSERT.
        private async Task CalculateAndInsertDailyBalancesAsync(DateTime processDate)
        {
            string currentDateStr = processDate.ToString("yyyy-MM-dd");
            string previousDateStr = processDate.AddDays(-1).ToString("yyyy-MM-dd");

            Console.WriteLine($"Processing EOD Balances for: {currentDateStr}");
            Console.WriteLine($"Using previous day's balances from: {previousDateStr}");

            // --- Part 1: DELETE existing data for the current processing date ---
            // This step ensures idempotency: if the process runs multiple times for the same date,
            // it will always replace the data for that day, matching Hive's INSERT OVERWRITE PARTITION behavior.
            string deleteSql = $@"
                DELETE FROM `{_projectId}.{_datasetId}.fact_daily_balances`
                WHERE balance_date = @current_date_param;
            ";

            var deleteParameters = new[]
            {
                new BigQueryParameter("current_date_param", BigQueryDbType.Date, currentDateStr)
            };
            await ExecuteBigQuerySqlAsync(deleteSql, deleteParameters, $"Deleting existing data for {currentDateStr}");


            // --- Part 2: Calculate and INSERT new daily balances ---
            // The logic from the HQL script is adapted here. The temporary table `tmp_daily_movements_stg`
            // is replaced by a Common Table Expression (CTE) named `DailyMovements` for efficiency and readability in BigQuery.
            string insertSql = $@"
                INSERT INTO `{_projectId}.{_datasetId}.fact_daily_balances` (
                    account_id, customer_id, account_type, open_date, currency_code,
                    beginning_balance, total_credits, total_debits, ending_balance,
                    interest_accrued, is_overdrawn, etl_timestamp, balance_date, region_code
                )
                WITH DailyMovements AS (
                    -- Calculate total credits per account for the current day
                    WITH credited AS (
                        SELECT
                            destination_account_id AS account_id,
                            SUM(amount_base_currency) AS total_credits
                        FROM `{_projectId}.{_datasetId}.fact_transactions`
                        WHERE trx_date = @current_date_param -- Filter for the current processing date
                        AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT') -- Exclude specific transaction types
                        GROUP BY destination_account_id
                    ),
                    -- Calculate total debits per account for the current day
                    debited AS (
                        SELECT
                            source_account_id AS account_id,
                            SUM(amount_base_currency) AS total_debits
                        FROM `{_projectId}.{_datasetId}.fact_transactions`
                        WHERE trx_date = @current_date_param -- Filter for the current processing date
                        AND transaction_type NOT IN ('REVERSAL_CREDIT') -- Exclude specific transaction types
                        GROUP BY source_account_id
                    )
                    -- Combine credits and debits using FULL OUTER JOIN to capture all accounts involved
                    SELECT
                        COALESCE(c.account_id, d.account_id) AS account_id,
                        COALESCE(c.total_credits, 0.0) AS total_credits,
                        COALESCE(d.total_debits, 0.0) AS total_debits
                    FROM credited c
                    FULL OUTER JOIN debited d ON c.account_id = d.account_id
                )
                SELECT
                    a.account_id,
                    a.customer_id,
                    a.account_type,
                    a.open_date,
                    a.currency_code,

                    -- Beginning Balance is the previous day's ending balance, defaulting to 0 for new accounts or the first day
                    COALESCE(prev.ending_balance, 0.0) AS beginning_balance,

                    -- Daily Movements calculated above
                    COALESCE(m.total_credits, 0.0) AS total_credits,
                    COALESCE(m.total_debits, 0.0) AS total_debits,

                    -- Ending Balance Calculation: Previous EOD + Credits - Debits
                    (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,

                    -- Simple interest calculation (mock logic: 0.05% APY / 365, only for SAVINGS accounts with positive balance)
                    CASE
                        WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0
                        THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)
                        ELSE 0.0
                    END AS interest_accrued,

                    -- Overdrawn flag: true if ending balance is negative
                    CASE
                        WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0
                        THEN TRUE
                        ELSE FALSE
                    END AS is_overdrawn,

                    CURRENT_TIMESTAMP() AS etl_timestamp, -- Record when this calculation was performed

                    DATE(@current_date_param) AS balance_date, -- The date this EOD balance applies to (partition key)
                    COALESCE(a.region_code, 'UN') AS region_code -- Account region (clustering key)

                FROM `{_projectId}.{_datasetId}.dim_accounts` a
                -- LEFT JOIN to get previous day's ending balance
                LEFT JOIN `{_projectId}.{_datasetId}.fact_daily_balances` prev
                    ON a.account_id = prev.account_id
                    AND prev.balance_date = DATE(@previous_date_param) -- Join with previous day's partition

                -- LEFT JOIN with today's movements from the CTE
                LEFT JOIN DailyMovements m
                    ON a.account_id = m.account_id
                WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT'); -- Only process active/relevant accounts
            ";

            var insertParameters = new[]
            {
                new BigQueryParameter("current_date_param", BigQueryDbType.Date, currentDateStr),
                new BigQueryParameter("previous_date_param", BigQueryDbType.Date, previousDateStr)
            };
            await ExecuteBigQuerySqlAsync(insertSql, insertParameters, $"Calculating and inserting balances for {currentDateStr}");
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            // --- Configuration ---
            // Replace with your actual Google Cloud Project ID and BigQuery Dataset ID.
            // Best practice: retrieve these from environment variables or a configuration service.
            string projectId = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT_ID") ?? "your-gcp-project-id"; 
            string datasetId = Environment.GetEnvironmentVariable("BIGQUERY_DATASET_ID") ?? "fin_core"; 

            if (projectId == "your-gcp-project-id")
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("WARNING: GOOGLE_CLOUD_PROJECT_ID environment variable not set or defaulted. Using placeholder projectId. " +
                                  "Please update Program.cs or set the environment variable for your project.");
                Console.ResetColor();
            }

            // Determine the processing date
            DateTime processDate;
            if (args.Length > 0 && DateTime.TryParse(args[0], out processDate))
            {
                Console.WriteLine($"Processing date provided as command-line argument: {processDate:yyyy-MM-dd}");
            }
            else
            {
                // Default to yesterday's date if no argument is provided, common for EOD batch jobs.
                processDate = DateTime.Today.AddDays(-1); 
                Console.WriteLine($"No processing date provided. Defaulting to yesterday: {processDate:yyyy-MM-dd}");
            }

            var calculator = new DailyBalanceCalculator(projectId, datasetId);
            await calculator.RunDailyBalanceProcessAsync(processDate);

            Console.WriteLine("\nPress any key to exit.");
            Console.ReadKey();
        }
    }
}
```

---

### How to Run:

1.  **Save the Code**: Save the C# code as `Program.cs` in a new .NET console application project (e.g., `FinDailyBalancesBigQuery.csproj`).
2.  **Set Project ID and Dataset ID**:
    *   Either replace `"your-gcp-project-id"` and `"fin_core"` directly in `Program.cs` with your actual Google Cloud Project ID and BigQuery Dataset ID.
    *   **Recommended**: Set environment variables:
        ```bash
        set GOOGLE_CLOUD_PROJECT_ID=your-gcp-project-id
        set BIGQUERY_DATASET_ID=fin_core
        # On Linux/macOS:
        export GOOGLE_CLOUD_PROJECT_ID=your-gcp-project-id
        export BIGQUERY_DATASET_ID=fin_core
        ```
3.  **Authenticate**: Ensure your local machine or deployment environment is authenticated to Google Cloud.
    *   For local development: Open your terminal and run `gcloud auth application-default login`.
4.  **Build and Run**:
    ```bash
    dotnet build
    dotnet run "2024-01-01" # Pass the desired processing date as an argument (optional, defaults to yesterday)
    ```

This C# application now effectively replicates the EOD balance calculation logic using BigQuery, adhering to BigQuery's data model and best practices for query execution and idempotency.