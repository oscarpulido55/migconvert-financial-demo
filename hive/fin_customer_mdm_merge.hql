This conversion will involve a few key changes:
1.  **Language:** Python/HQL to C#.
2.  **Database:** Hive (which is SQL, but often used via tools like Spark SQL or similar SQL-on-Hadoop engines) to Google BigQuery.
    *   BigQuery SQL dialect changes: `GENERATE_UUID()` instead of `reflect(...)`, specific timestamp/date handling, backticks for table names.
    *   BigQuery also highly favors `MERGE` statements for SCD operations, which is more efficient than separate `INSERT` statements from a staged view, but I'll stick to the original HQL's multi-step `INSERT` pattern for a more direct conversion unless explicitly asked for a `MERGE`.
    *   BigQuery uses `CREATE TEMP TABLE` for temporary tables, which are session-scoped and automatically cleaned up, replacing the `DROP VIEW IF EXISTS`/`CREATE VIEW` pattern.

Let's break this down into the BigQuery SQL first, then the C# code to execute it.

---

### BigQuery SQL (Equivalent to the HQL Logic)

First, the target table definition (adapting Hive's `STORED AS ORC` to BigQuery's native column-store and naming conventions).

```sql
-- ==============================================================================
-- Target Table Definition for Customers (SCD Type 2) in BigQuery
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `fin_core.dim_customers_scd2` (
    customer_surrogate_key STRING OPTIONS(description="Unique identifier for each customer version (UUID)"),
    customer_id STRING OPTIONS(description="Natural key for the customer"),
    first_name STRING,
    last_name STRING,
    email_address STRING,
    phone_number STRING,
    residential_address STRING,
    marital_status STRING,
    effective_start_date TIMESTAMP OPTIONS(description="Start date of the customer record's validity"),
    effective_end_date TIMESTAMP OPTIONS(description="End date of the customer record's validity"),
    is_active BOOLEAN OPTIONS(description="Indicates if this is the currently active record for the customer")
)
-- Recommended options for BigQuery for large tables
-- PARTITION BY DATE(effective_start_date) -- Example: partition by the start date for better query performance
-- CLUSTER BY customer_id                  -- Example: cluster by customer_id for efficient lookup on customer_id
OPTIONS(
  description = 'Master Data Management (MDM) table for Customers (SCD Type 2)'
);

-- ==============================================================================
-- BigQuery SQL Logic for SCD Type 2 Upsert
-- This uses a temporary table to stage changes, mimicking the HQL VIEW approach.
-- `@runDate` and `@runTimestampStartOfDay` are parameters passed from C#.
-- ==============================================================================

-- Step 1: Create a temporary staging table to identify changes
-- This replaces the `CREATE VIEW default.vw_scd_transform_stg` and encapsulates
-- the full outer join logic. Temporary tables are automatically cleaned up.
CREATE TEMP TABLE `scd_transform_stg_temp` AS
WITH active_records AS (
    SELECT
        customer_surrogate_key,
        customer_id,
        first_name,
        last_name,
        email_address,
        phone_number,
        residential_address,
        marital_status,
        effective_start_date,
        effective_end_date,
        is_active
    FROM `fin_core.dim_customers_scd2`
    WHERE is_active = TRUE
),
incoming_updates AS (
    -- Deduplicate incoming data for the current run date, picking the latest record
    SELECT
        customer_id,
        first_name,
        last_name,
        email_address,
        phone_number,
        residential_address,
        marital_status,
        timestamp AS update_ts
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) as rn
        FROM `fin_landing.customer_updates` -- Assuming `fin_landing` is also a dataset name, if not, adjust path.
        WHERE DATE(timestamp) = @runDate -- Parameterized current processing date (e.g., '2024-01-01')
    )
    WHERE rn = 1
)
-- FULL OUTER JOIN handles Inserts, Updates, and Unchanged
SELECT
    COALESCE(i.customer_id, a.customer_id) as customer_id,

    -- Case 1: Brand new customer (Insert) OR Updated Customer (Insert new active row)
    CASE WHEN i.customer_id IS NOT NULL THEN i.first_name ELSE a.first_name END AS first_name_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.last_name ELSE a.last_name END AS last_name_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.email_address ELSE a.email_address END AS email_address_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.phone_number ELSE a.phone_number END AS phone_number_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.residential_address ELSE a.residential_address END AS residential_address_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.marital_status ELSE a.marital_status END AS marital_status_new,

    -- Evaluate if a change actually occurred
    CASE
        WHEN a.customer_id IS NULL THEN 'INSERT' -- Brand New
        WHEN i.customer_id IS NOT NULL AND (
             COALESCE(i.last_name, '') != COALESCE(a.last_name, '') OR
             COALESCE(i.email_address, '') != COALESCE(a.email_address, '') OR
             COALESCE(i.residential_address, '') != COALESCE(a.residential_address, '') OR
             COALESCE(i.marital_status, '') != COALESCE(a.marital_status, '')
        ) THEN 'UPDATE'
        WHEN i.customer_id IS NULL THEN 'NO_CHANGE_ACTIVE' -- Existing active record not in today's delta, let it pass through as is.
        ELSE 'NO_CHANGE_INCOMING' -- Incoming data matches current active
    END as change_type,

    -- Retain old record details to age it out
    a.customer_surrogate_key AS old_sk,
    a.first_name AS first_name_old,
    a.last_name AS last_name_old,
    a.email_address AS email_address_old,
    a.phone_number AS phone_number_old,
    a.residential_address AS residential_address_old,
    a.marital_status AS marital_status_old,
    a.effective_start_date AS old_start_date

FROM incoming_updates i
FULL OUTER JOIN active_records a ON i.customer_id = a.customer_id;

-- Step 2: Insert the retired rows (closing the effective_end_date and setting is_active = false)
INSERT INTO `fin_core.dim_customers_scd2` (
    customer_surrogate_key,
    customer_id,
    first_name,
    last_name,
    email_address,
    phone_number,
    residential_address,
    marital_status,
    effective_start_date,
    effective_end_date,
    is_active
)
SELECT
    old_sk,
    customer_id,
    first_name_old,
    last_name_old,
    email_address_old,
    phone_number_old,
    residential_address_old,
    marital_status_old,
    old_start_date,
    @runTimestampStartOfDay, -- Parameterized end date for the retired record
    FALSE
FROM `scd_transform_stg_temp`
WHERE change_type = 'UPDATE';

-- Step 3: Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
INSERT INTO `fin_core.dim_customers_scd2` (
    customer_surrogate_key,
    customer_id,
    first_name,
    last_name,
    email_address,
    phone_number,
    residential_address,
    marital_status,
    effective_start_date,
    effective_end_date,
    is_active
)
SELECT
    GENERATE_UUID(), -- BigQuery function to generate a UUID
    customer_id,
    first_name_new,
    last_name_new,
    email_address_new,
    phone_number_new,
    residential_address_new,
    marital_status_new,
    @runTimestampStartOfDay, -- Parameterized start date for the new active record
    TIMESTAMP('9999-12-31 23:59:59'), -- Far future date for active records
    TRUE
FROM `scd_transform_stg_temp`
WHERE change_type IN ('INSERT', 'UPDATE');

-- Note: 'NO_CHANGE_ACTIVE' and 'NO_CHANGE_INCOMING' records are handled as follows:
-- 'NO_CHANGE_ACTIVE': These records exist in the target as active, but did not appear in today's incoming updates.
--                     The logic here implicitly keeps them active without any modification.
-- 'NO_CHANGE_INCOMING': These records appeared in today's incoming updates, but no significant changes were detected
--                       compared to their active version in the target. These are also implicitly kept active as-is.

-- The temporary table `scd_transform_stg_temp` is automatically dropped at the end of the session.
```

---

### C# Code (Executing BigQuery SQL)

This C# code will use the `Google.Cloud.BigQuery.V2` NuGet package to interact with BigQuery.

First, ensure you have the NuGet package installed:
`Install-Package Google.Cloud.BigQuery.V2`

You'll also need to have Google Cloud authentication set up (e.g., via `gcloud auth application-default login` or by providing a service account key JSON file in an environment variable `GOOGLE_APPLICATION_CREDENTIALS`).

```csharp
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

public class CustomerMdmService
{
    private readonly string _projectId;
    private readonly string _datasetId;
    private readonly BigQueryClient _bigQueryClient;

    /// <summary>
    /// Initializes a new instance of the CustomerMdmService.
    /// </summary>
    /// <param name="projectId">Your Google Cloud Project ID.</param>
    /// <param name="datasetId">The BigQuery Dataset ID where tables like dim_customers_scd2 and customer_updates reside.</param>
    public CustomerMdmService(string projectId, string datasetId)
    {
        _projectId = projectId;
        _datasetId = datasetId;
        _bigQueryClient = BigQueryClient.Create(_projectId); // Initialize client once
    }

    /// <summary>
    /// Ensures the SCD Type 2 customer dimension table exists in BigQuery.
    /// </summary>
    public async Task CreateScdCustomerTableIfNotExistsAsync()
    {
        string createTableSql = $@"
            CREATE TABLE IF NOT EXISTS `{_datasetId}.dim_customers_scd2` (
                customer_surrogate_key STRING OPTIONS(description=""Unique identifier for each customer version (UUID)""),
                customer_id STRING OPTIONS(description=""Natural key for the customer""),
                first_name STRING,
                last_name STRING,
                email_address STRING,
                phone_number STRING,
                residential_address STRING,
                marital_status STRING,
                effective_start_date TIMESTAMP OPTIONS(description=""Start date of the customer record's validity""),
                effective_end_date TIMESTAMP OPTIONS(description=""End date of the customer record's validity""),
                is_active BOOLEAN OPTIONS(description=""Indicates if this is the currently active record for the customer"")
            )
            -- Consider adding partitioning/clustering options here for large tables, e.g.:
            -- PARTITION BY DATE(effective_start_date)
            -- CLUSTER BY customer_id
            OPTIONS(
              description = 'Master Data Management (MDM) table for Customers (SCD Type 2)'
            );";

        Console.WriteLine("Ensuring dim_customers_scd2 table exists...");
        await _bigQueryClient.ExecuteQueryAsync(createTableSql);
        Console.WriteLine("dim_customers_scd2 table status: checked/created.");
    }

    /// <summary>
    /// Performs an SCD Type 2 upsert operation for customer data in BigQuery.
    /// </summary>
    /// <param name="runDate">The date for which to process incoming updates (e.g., '2024-01-01').</param>
    /// <param name="landingDatasetId">The BigQuery Dataset ID where `customer_updates` table resides.
    ///                                If same as target dataset, use `_datasetId`.</param>
    public async Task ExecuteScdType2UpsertAsync(DateOnly runDate, string landingDatasetId)
    {
        Console.WriteLine($"Starting SCD Type 2 Upsert for run date: {runDate}");

        // Convert runDate to a DateTime at the start of the day for TIMESTAMP parameters
        DateTime runTimestampStartOfDay = runDate.ToDateTime(TimeOnly.MinValue);

        // Parameters for BigQuery queries
        var queryParameters = new List<BigQueryParameter>
        {
            BigQueryParameter.FromDateTime("runDate", runTimestampStartOfDay), // For DATE(timestamp) = @runDate
            BigQueryParameter.FromDateTime("runTimestampStartOfDay", runTimestampStartOfDay)
        };

        // Step 1: Create a temporary staging table
        string createStagingTableSql = $@"
            CREATE TEMP TABLE `scd_transform_stg_temp` AS
            WITH active_records AS (
                SELECT * FROM `{_datasetId}.dim_customers_scd2` WHERE is_active = TRUE
            ),
            incoming_updates AS (
                SELECT
                    customer_id, first_name, last_name, email_address, phone_number,
                    residential_address, marital_status, timestamp AS update_ts
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) as rn
                    FROM `{landingDatasetId}.customer_updates`
                    WHERE DATE(timestamp) = @runDate
                ) WHERE rn = 1
            )
            SELECT
                COALESCE(i.customer_id, a.customer_id) as customer_id,
                CASE WHEN i.customer_id IS NOT NULL THEN i.first_name ELSE a.first_name END AS first_name_new,
                CASE WHEN i.customer_id IS NOT NULL THEN i.last_name ELSE a.last_name END AS last_name_new,
                CASE WHEN i.customer_id IS NOT NULL THEN i.email_address ELSE a.email_address END AS email_address_new,
                CASE WHEN i.customer_id IS NOT NULL THEN i.phone_number ELSE a.phone_number END AS phone_number_new,
                CASE WHEN i.customer_id IS NOT NULL THEN i.residential_address ELSE a.residential_address END AS residential_address_new,
                CASE WHEN i.customer_id IS NOT NULL THEN i.marital_status ELSE a.marital_status END AS marital_status_new,
                CASE
                    WHEN a.customer_id IS NULL THEN 'INSERT'
                    WHEN i.customer_id IS NOT NULL AND (
                         COALESCE(i.last_name, '') != COALESCE(a.last_name, '') OR
                         COALESCE(i.email_address, '') != COALESCE(a.email_address, '') OR
                         COALESCE(i.residential_address, '') != COALESCE(a.residential_address, '') OR
                         COALESCE(i.marital_status, '') != COALESCE(a.marital_status, '')
                    ) THEN 'UPDATE'
                    WHEN i.customer_id IS NULL THEN 'NO_CHANGE_ACTIVE'
                    ELSE 'NO_CHANGE_INCOMING'
                END as change_type,
                a.customer_surrogate_key AS old_sk,
                a.first_name AS first_name_old,
                a.last_name AS last_name_old,
                a.email_address AS email_address_old,
                a.phone_number AS phone_number_old,
                a.residential_address AS residential_address_old,
                a.marital_status AS marital_status_old,
                a.effective_start_date AS old_start_date
            FROM incoming_updates i
            FULL OUTER JOIN active_records a ON i.customer_id = a.customer_id;
        ";
        Console.WriteLine("Executing Step 1: Creating temporary staging table...");
        await _bigQueryClient.ExecuteQueryAsync(createStagingTableSql, queryParameters.ToArray());
        Console.WriteLine("Step 1 Complete: Temporary staging table created.");

        // Step 2: Insert the retired rows (closing the effective_end_date and setting is_active = false)
        string insertRetiredSql = $@"
            INSERT INTO `{_datasetId}.dim_customers_scd2` (
                customer_surrogate_key, customer_id, first_name, last_name, email_address,
                phone_number, residential_address, marital_status, effective_start_date,
                effective_end_date, is_active
            )
            SELECT
                old_sk, customer_id, first_name_old, last_name_old, email_address_old,
                phone_number_old, residential_address_old, marital_status_old, old_start_date,
                @runTimestampStartOfDay,
                FALSE
            FROM `scd_transform_stg_temp`
            WHERE change_type = 'UPDATE';
        ";
        Console.WriteLine("Executing Step 2: Retiring old records for updated customers...");
        await _bigQueryClient.ExecuteQueryAsync(insertRetiredSql, queryParameters.ToArray());
        Console.WriteLine("Step 2 Complete: Old records retired.");

        // Step 3: Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
        string insertNewActiveSql = $@"
            INSERT INTO `{_datasetId}.dim_customers_scd2` (
                customer_surrogate_key, customer_id, first_name, last_name, email_address,
                phone_number, residential_address, marital_address, effective_start_date,
                effective_end_date, is_active
            )
            SELECT
                GENERATE_UUID(),
                customer_id,
                first_name_new, last_name_new, email_address_new,
                phone_number_new, residential_address_new, marital_status_new,
                @runTimestampStartOfDay,
                TIMESTAMP('9999-12-31 23:59:59'),
                TRUE
            FROM `scd_transform_stg_temp`
            WHERE change_type IN ('INSERT', 'UPDATE');
        ";
        Console.WriteLine("Executing Step 3: Inserting new and updated-active customer records...");
        await _bigQueryClient.ExecuteQueryAsync(insertNewActiveSql, queryParameters.ToArray());
        Console.WriteLine("Step 3 Complete: New and updated-active records inserted.");

        Console.WriteLine($"SCD Type 2 Upsert for run date {runDate} finished successfully.");
    }
}

// Example Usage:
public class Program
{
    public static async Task Main(string[] args)
    {
        string projectId = "your-gcp-project-id"; // Replace with your GCP Project ID
        string targetDatasetId = "fin_core";      // Replace with your target dataset ID
        string landingDatasetId = "fin_landing";  // Replace with your landing dataset ID if different from target, else use targetDatasetId

        // For demonstration, processing today's date
        DateOnly today = DateOnly.FromDateTime(DateTime.Today);

        var mdmService = new CustomerMdmService(projectId, targetDatasetId);

        try
        {
            await mdmService.CreateScdCustomerTableIfNotExistsAsync(); // Optional: Ensure table exists
            await mdmService.ExecuteScdType2UpsertAsync(today, landingDatasetId);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }

        Console.WriteLine("Press any key to exit.");
        Console.ReadKey();
    }
}
```

---

### Explanation of Changes and Considerations:

1.  **BigQuery SQL Dialect:**
    *   Table names are enclosed in backticks (e.g., `` `fin_core.dim_customers_scd2` ``) instead of double quotes or no quotes in Hive.
    *   `STRING` type is used for UUIDs (BigQuery doesn't have a native UUID type but `GENERATE_UUID()` returns a `STRING`).
    *   `TIMESTAMP` for dates with time. `DATE()` function to extract the date part from a `TIMESTAMP`.
    *   `reflect("java.util.UUID", "randomUUID")` is replaced with BigQuery's native `GENERATE_UUID()`.
    *   `CAST('2024-01-01 00:00:00' AS TIMESTAMP)` is replaced with the `@runTimestampStartOfDay` parameter, ensuring consistency and making the script dynamic. The fixed `'9999-12-31 23:59:59'` remains.
    *   `CREATE TEMP TABLE` is used instead of `CREATE VIEW` for the staging logic, as temp tables are more suitable for multi-step data transformations in BigQuery. They are automatically cleaned up at the end of the query session.

2.  **C# BigQuery Client (`Google.Cloud.BigQuery.V2`):**
    *   Uses `BigQueryClient.Create()` to connect.
    *   `ExecuteQueryAsync()` runs the SQL queries.
    *   `BigQueryParameter` is used to safely pass dynamic values like `runDate` to the SQL queries, preventing SQL injection and ensuring correct type handling.
    *   The `DateOnly` struct (available in .NET 6+) is used for `runDate` to represent a date without a time component, then converted to `DateTime` for `BigQueryParameter`.
    *   The BigQuery operations are asynchronous, so `async`/`await` are used throughout the C# code.

3.  **SCD Logic Equivalence:**
    *   The core logic with `FULL OUTER JOIN` and `CASE` statements is preserved to identify 'INSERT', 'UPDATE', and 'NO_CHANGE' types.
    *   The two separate `INSERT` statements to first "retire" old records and then "insert" new/updated active records are also preserved, exactly mirroring the original Hive SQL's behavior.

4.  **Error Handling and Logging:**
    *   Basic `Console.WriteLine` statements are added for progress and rudimentary logging.
    *   A `try-catch` block handles potential exceptions during BigQuery operations.

5.  **Configuration:**
    *   `projectId`, `datasetId`, `landingDatasetId` are placeholders that need to be replaced with your actual GCP project and dataset IDs. `landingDatasetId` specifically refers to the dataset where your `customer_updates` table is located, which might be different from your core dimension tables.

This C# solution provides a robust and idiomatic way to implement the specified SCD Type 2 logic using Google BigQuery.