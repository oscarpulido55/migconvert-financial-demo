The request involves two main transformations:
1.  **HQL (Hive Query Language) to BigQuery SQL:** This means adjusting syntax, data types, and concepts like partitioning and temporary tables to BigQuery's standards.
2.  **Python (Implicit Orchestration) to C#:** This involves creating a C# application that uses the Google Cloud BigQuery client library to connect, execute queries, and manage the workflow.

---

## 1. BigQuery SQL Transformation

Here's the HQL converted to BigQuery Standard SQL, incorporating BigQuery's syntax and features.

**Assumptions:**
*   `fin_core` will be a BigQuery dataset.
*   `fact_transactions` and `dim_accounts` tables also reside within the `fin_core` dataset (e.g., `your-gcp-project-id.fin_core.fact_transactions`).
*   `trx_date` in `fact_transactions` is a `DATE` type column. If it's a `TIMESTAMP` column, you'd use `DATE(trx_timestamp)` in the `WHERE` clause.

### SQL Queries for BigQuery

```sql
-- Query 1: Ensure the Daily Balance Snapshot Table exists
-- HQL equivalent: CREATE TABLE IF NOT EXISTS fin_core.fact_daily_balances (...)
CREATE TABLE IF NOT EXISTS `{projectId}.{datasetId}.fact_daily_balances` (
    account_id STRING OPTIONS(description="Unique identifier for the account"),
    customer_id STRING OPTIONS(description="Identifier for the account owner"),
    account_type STRING OPTIONS(description="Type of account (CHECKING, SAVINGS, LOAN)"),
    open_date DATE OPTIONS(description="Date the account was opened"),
    currency_code STRING OPTIONS(description="Base currency of the account"),
    beginning_balance NUMERIC OPTIONS(description="Balance at the start of the day"),
    total_credits NUMERIC OPTIONS(description="Total value of incoming funds"),
    total_debits NUMERIC OPTIONS(description="Total value of outgoing funds"),
    ending_balance NUMERIC OPTIONS(description="Balance at the end of the day"),
    interest_accrued NUMERIC OPTIONS(description="Daily interest accrued"),
    is_overdrawn BOOL OPTIONS(description="Flag indicating if the account is in negative balance"),
    etl_timestamp TIMESTAMP
)
PARTITION BY balance_date
CLUSTER BY region_code
OPTIONS(
    description="Stores the End of Day balances for all accounts"
);

-- Query 2: Delete existing data for the target partition
-- This step ensures 'overwrite partition' behavior in BigQuery.
DELETE FROM `{projectId}.{datasetId}.fact_daily_balances`
WHERE balance_date = @balance_date_param; -- @balance_date_param will be provided from C#

-- Query 3: Calculate End of Day Balances and Insert
-- HQL equivalent: INSERT OVERWRITE TABLE ... SELECT ... with CTEs replacing temporary table.
INSERT `{projectId}.{datasetId}.fact_daily_balances` (
    account_id,
    customer_id,
    account_type,
    open_date,
    currency_code,
    beginning_balance,
    total_credits,
    total_debits,
    ending_balance,
    interest_accrued,
    is_overdrawn,
    etl_timestamp,
    balance_date,
    region_code
)
WITH credited AS (
    SELECT
        destination_account_id AS account_id,
        SUM(amount_base_currency) AS total_credits
    FROM `{projectId}.{datasetId}.fact_transactions`
    WHERE trx_date = @balance_date_param -- Assuming trx_date is a DATE column
      AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
    GROUP BY destination_account_id
),
debited AS (
    SELECT
        source_account_id AS account_id,
        SUM(amount_base_currency) AS total_debits
    FROM `{projectId}.{datasetId}.fact_transactions`
    WHERE trx_date = @balance_date_param -- Assuming trx_date is a DATE column
      AND transaction_type NOT IN ('REVERSAL_CREDIT')
    GROUP BY source_account_id
),
tmp_daily_movements_stg AS (
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

    COALESCE(prev.ending_balance, 0.0) AS beginning_balance,
    COALESCE(m.total_credits, 0.0) AS total_credits,
    COALESCE(m.total_debits, 0.0) AS total_debits,
    (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,

    CASE
        WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0
        THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)
        ELSE 0.0
    END AS interest_accrued,

    CASE
        WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0
        THEN true
        ELSE false
    END AS is_overdrawn,

    CURRENT_TIMESTAMP() AS etl_timestamp,

    @balance_date_param AS balance_date,
    COALESCE(a.region_code, 'UN') AS region_code

FROM `{projectId}.{datasetId}.dim_accounts` a
LEFT JOIN `{projectId}.{datasetId}.fact_daily_balances` prev
    ON a.account_id = prev.account_id
    AND prev.balance_date = DATE_SUB(@balance_date_param, INTERVAL 1 DAY)
LEFT JOIN tmp_daily_movements_stg m
    ON a.account_id = m.account_id
WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');
```

---

## 2. C# Implementation

This C# code demonstrates how to use the `Google.Cloud.BigQuery.V2` client library to execute the BigQuery SQL queries.

**Prerequisites:**

1.  **Install NuGet Package:**
    ```bash
    dotnet add package Google.Cloud.BigQuery.V2
    ```
2.  **Google Cloud Authentication:** Ensure your environment is authenticated to Google Cloud.
    *   **Recommended for development:** `gcloud auth application-default login` (This will create credentials locally that your application can automatically pick up).
    *   **For production:** Use Service Account keys or Managed Identities on GCP services (e.g., Cloud Run, Cloud Functions, GKE).

```csharp
using Google.Cloud.BigQuery.V2;
using System;
using System.Linq;
using System.Threading.Tasks;

public class FinancialDailyBalanceCalculator
{
    private readonly string _projectId;
    private readonly string _datasetId;
    private readonly BigQueryClient _bigQueryClient;

    /// <summary>
    /// Initializes a new instance of the FinancialDailyBalanceCalculator.
    /// </summary>
    /// <param name="projectId">Your Google Cloud Project ID.</param>
    /// <param name="datasetId">The BigQuery Dataset ID (e.g., "fin_core").</param>
    public FinancialDailyBalanceCalculator(string projectId, string datasetId)
    {
        _projectId = projectId ?? throw new ArgumentNullException(nameof(projectId));
        _datasetId = datasetId ?? throw new ArgumentNullException(nameof(datasetId));
        
        // The BigQueryClient will automatically pick up authentication credentials
        // from the environment (e.g., GOOGLE_APPLICATION_CREDENTIALS, gcloud CLI config).
        _bigQueryClient = BigQueryClient.Create(_projectId);
        Console.WriteLine($"BigQuery client initialized for Project: '{_projectId}' and Dataset: '{_datasetId}'.");
    }

    /// <summary>
    /// Ensures the target BigQuery dataset exists.
    /// </summary>
    private async Task EnsureDatasetExistsAsync()
    {
        try
        {
            await _bigQueryClient.GetDatasetAsync(_datasetId);
            Console.WriteLine($"Dataset '{_datasetId}' already exists.");
        }
        catch (Google.GoogleApiException e) when (e.Error.Code == 404)
        {
            Console.WriteLine($"Dataset '{_datasetId}' does not exist. Creating...");
            var dataset = new BigQueryDataset { Reference = new DatasetReference(_projectId, _datasetId) };
            await _bigQueryClient.CreateDatasetAsync(dataset);
            Console.WriteLine($"Dataset '{_datasetId}' created successfully.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error ensuring dataset exists: {ex.Message}");
            throw; // Re-throw to indicate a critical setup failure
        }
    }

    /// <summary>
    /// Calculates and updates end-of-day balances for all banking accounts for a given date.
    /// </summary>
    /// <param name="balanceDate">The date for which to calculate the balances.</param>
    public async Task CalculateDailyBalancesAsync(DateTime balanceDate)
    {
        Console.WriteLine($"\n--- Starting daily balance calculation for date: {balanceDate:yyyy-MM-dd} ---");

        await EnsureDatasetExistsAsync();

        // Format the date for SQL parameter (BigQuery DATE type expects 'YYYY-MM-DD')
        string balanceDateSqlParam = balanceDate.ToString("yyyy-MM-dd");

        // 1. Create the Daily Balance Snapshot Table if it doesn't exist
        Console.WriteLine("Executing: Create fact_daily_balances table if not exists...");
        string createTableSql = $@"
            CREATE TABLE IF NOT EXISTS `{_projectId}.{_datasetId}.fact_daily_balances` (
                account_id STRING OPTIONS(description=""Unique identifier for the account""),
                customer_id STRING OPTIONS(description=""Identifier for the account owner""),
                account_type STRING OPTIONS(description=""Type of account (CHECKING, SAVINGS, LOAN)""),
                open_date DATE OPTIONS(description=""Date the account was opened""),
                currency_code STRING OPTIONS(description=""Base currency of the account""),
                beginning_balance NUMERIC OPTIONS(description=""Balance at the start of the day""),
                total_credits NUMERIC OPTIONS(description=""Total value of incoming funds""),
                total_debits NUMERIC OPTIONS(description=""Total value of outgoing funds""),
                ending_balance NUMERIC OPTIONS(description=""Balance at the end of the day""),
                interest_accrued NUMERIC OPTIONS(description=""Daily interest accrued""),
                is_overdrawn BOOL OPTIONS(description=""Flag indicating if the account is in negative balance""),
                etl_timestamp TIMESTAMP
            )
            PARTITION BY balance_date
            CLUSTER BY region_code
            OPTIONS(
                description=""Stores the End of Day balances for all accounts""
            );";
        await ExecuteBigQueryJobAsync(createTableSql, "CreateTableFactDailyBalances");
        Console.WriteLine("Fact_daily_balances table checked/created.");


        // 2. Delete existing data for the target partition
        // This simulates the HQL's 'INSERT OVERWRITE TABLE ... PARTITION' behavior.
        Console.WriteLine($"Executing: Deleting existing data for balance_date: {balanceDateSqlParam}...");
        string deletePartitionSql = $@"
            DELETE FROM `{_projectId}.{_datasetId}.fact_daily_balances`
            WHERE balance_date = @balance_date_param;";

        var deleteParameters = new[]
        {
            new BigQueryParameter("balance_date_param", BigQueryDbType.Date, balanceDateSqlParam)
        };
        await ExecuteBigQueryJobAsync(deletePartitionSql, "DeletePartitionData", deleteParameters);
        Console.WriteLine($"Existing data for balance_date: {balanceDateSqlParam} deleted (if any).");


        // 3. Calculate End of Day Balances and Insert into the table
        Console.WriteLine($"Executing: Inserting daily balances for date: {balanceDateSqlParam}...");
        string insertBalancesSql = $@"
            INSERT `{_projectId}.{_datasetId}.fact_daily_balances` (
                account_id,
                customer_id,
                account_type,
                open_date,
                currency_code,
                beginning_balance,
                total_credits,
                total_debits,
                ending_balance,
                interest_accrued,
                is_overdrawn,
                etl_timestamp,
                balance_date,
                region_code
            )
            WITH credited AS (
                SELECT
                    destination_account_id AS account_id,
                    SUM(amount_base_currency) AS total_credits
                FROM `{_projectId}.{_datasetId}.fact_transactions`
                WHERE trx_date = @balance_date_param -- Assuming trx_date is a DATE column
                  AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
                GROUP BY destination_account_id
            ),
            debited AS (
                SELECT
                    source_account_id AS account_id,
                    SUM(amount_base_currency) AS total_debits
                FROM `{_projectId}.{_datasetId}.fact_transactions`
                WHERE trx_date = @balance_date_param -- Assuming trx_date is a DATE column
                  AND transaction_type NOT IN ('REVERSAL_CREDIT')
                GROUP BY source_account_id
            ),
            tmp_daily_movements_stg AS (
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

                COALESCE(prev.ending_balance, 0.0) AS beginning_balance,
                COALESCE(m.total_credits, 0.0) AS total_credits,
                COALESCE(m.total_debits, 0.0) AS total_debits,
                (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,

                CASE
                    WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0
                    THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)
                    ELSE 0.0
                END AS interest_accrued,

                CASE
                    WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0
                    THEN true
                    ELSE false
                END AS is_overdrawn,

                CURRENT_TIMESTAMP() AS etl_timestamp,

                @balance_date_param AS balance_date,
                COALESCE(a.region_code, 'UN') AS region_code

            FROM `{_projectId}.{_datasetId}.dim_accounts` a
            LEFT JOIN `{_projectId}.{_datasetId}.fact_daily_balances` prev
                ON a.account_id = prev.account_id
                AND prev.balance_date = DATE_SUB(@balance_date_param, INTERVAL 1 DAY)
            LEFT JOIN tmp_daily_movements_stg m
                ON a.account_id = m.account_id
            WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');";

        var insertParameters = new[]
        {
            new BigQueryParameter("balance_date_param", BigQueryDbType.Date, balanceDateSqlParam)
        };
        await ExecuteBigQueryJobAsync(insertBalancesSql, "InsertDailyBalances", insertParameters);
        Console.WriteLine($"Daily balances for date: {balanceDateSqlParam} calculated and inserted successfully.");
        
        Console.WriteLine("\n--- Daily balance calculation process completed ---");
    }

    /// <summary>
    /// Executes a BigQuery SQL query as a job and waits for its completion.
    /// </summary>
    /// <param name="query">The SQL query string to execute.</param>
    /// <param name="jobName">A descriptive name for the BigQuery job (for logging).</param>
    /// <param name="parameters">Optional BigQuery parameters for the query.</param>
    /// <returns>A Task representing the asynchronous operation.</returns>
    private async Task ExecuteBigQueryJobAsync(string query, string jobName, BigQueryParameter[] parameters = null)
    {
        try
        {
            Console.WriteLine($"  Submitting BigQuery job: '{jobName}'...");
            
            var queryRequest = new QueryRequest
            {
                Query = query,
                UseLegacySql = false, // Always use standard SQL
                QueryParameters = parameters?.ToList() // Pass parameters if available
            };

            var job = await _bigQueryClient.CreateQueryJobAsync(queryRequest);

            // Wait for the job to complete. This is blocking. For very long-running jobs,
            // consider polling less frequently or separating job creation from waiting.
            Job completedJob = await job.PollUntilCompletedAsync();

            if (completedJob.Status.Errors != null && completedJob.Status.Errors.Any())
            {
                var errorMessages = string.Join(Environment.NewLine, completedJob.Status.Errors.Select(e => $"  - {e.Message}"));
                throw new Exception($"BigQuery job '{jobName}' failed. Errors: {Environment.NewLine}{errorMessages}");
            }
            Console.WriteLine($"  BigQuery job '{jobName}' completed successfully. Job ID: {job.Reference.JobId}");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error executing BigQuery job '{jobName}': {ex.Message}");
            // Optionally, log the full query for debugging purposes (be careful with sensitive data)
            // Console.Error.WriteLine($"  Query: {query}"); 
            throw; // Re-throw to propagate the error up the call stack
        }
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        // --- Configuration ---
        // IMPORTANT: Replace with your actual Google Cloud Project ID and BigQuery Dataset ID
        const string projectId = "your-gcp-project-id"; 
        const string datasetId = "fin_core"; // As per the HQL script's database name

        // --- Date to process ---
        // In a real-world scenario, this date would typically come from:
        // 1. Command-line arguments (`args`)
        // 2. Configuration file
        // 3. A scheduler (e.g., running daily for DateTime.Today.AddDays(-1))
        DateTime processDate = new DateTime(2024, 1, 1); 

        // --- Execution ---
        if (projectId == "your-gcp-project-id")
        {
            Console.Error.WriteLine("ERROR: Please update 'projectId' in Program.cs with your actual Google Cloud Project ID.");
            Environment.ExitCode = 1;
            return;
        }

        try
        {
            var calculator = new FinancialDailyBalanceCalculator(projectId, datasetId);
            await calculator.CalculateDailyBalancesAsync(processDate);
            Console.WriteLine("\nOverall process finished successfully.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"\n!!! An unhandled critical error occurred: {ex.Message}");
            Console.Error.WriteLine(ex.StackTrace);
            Environment.ExitCode = 1; // Indicate application failure
        }
    }
}
```