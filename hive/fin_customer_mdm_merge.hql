Okay, this is a substantial conversion! We'll translate the Hive SQL SCD Type 2 logic into C# code that interacts with Google BigQuery.

The C# code will primarily be responsible for:
1.  Connecting to BigQuery.
2.  Orchestrating the execution of BigQuery SQL statements.
3.  Passing dynamic parameters (like the `run_date`).

The core SCD Type 2 logic will remain in BigQuery SQL, adapted from Hive syntax.

**Assumptions:**

*   You have a Google Cloud Project with BigQuery enabled.
*   You have authentication set up for your C# application (e.g., using `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account key file, or running on GCP infrastructure that provides default credentials).
*   The `fin_landing.customer_updates` table exists in BigQuery with a `timestamp` column and the relevant customer data.
*   We'll use a `runDate` parameter (which was hardcoded to `2024-01-01` in your Hive SQL) for all date operations.

---

### 1. BigQuery SQL Equivalent (Adapted from Hive)

Let's first get the BigQuery SQL ready. I'll make the date dynamic using a `@runDate` parameter.

```sql
-- ==============================================================================
-- Module: fin_customer_mdm_merge.bqsql
-- Description: Master Data Management (MDM) module performing a Slowly Changing
-- Dimension (SCD) Type 2 upsert operation for Customers in BigQuery.
-- Adapted from Hive SQL logic using CTEs and standard SQL for clarity.
-- ==============================================================================

-- Core Target Table for Customers (SCD Type 2)
-- Using a temporary name `dim_customers_scd2_target` for setup purposes.
-- Replace `your_project_id.your_dataset_id` with your actual project and dataset.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.dim_customers_scd2` (
    customer_surrogate_key STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email_address STRING,
    phone_number STRING,
    residential_address STRING,
    marital_status STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

-- ==============================================================================
-- STEP 1: Transform and Identify Changes (equivalent to vw_scd_transform_stg)
-- ==============================================================================
-- This query will be executed first to generate a temporary result set.
-- We cannot use a 'CREATE VIEW' directly in a parameterized fashion,
-- so we will encapsulate the logic in a CTE structure or execute directly.

-- BigQuery SCD Type 2 Transformation Query
-- Placeholders: `@runDate` and `your_project_id.your_dataset_id`.

-- Define a temporary view or CTEs for processing
-- The C# code will execute this full query.
WITH
  active_records AS (
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
    FROM
      `your_project_id.your_dataset_id.dim_customers_scd2`
    WHERE
      is_active = TRUE
  ),
  incoming_updates AS (
    -- Deduplicate incoming just in case
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
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) AS rn
      FROM
        `your_project_id.your_dataset_id.customer_updates` -- Assuming landing table is also in your_dataset_id
      WHERE
        DATE(timestamp) = CAST(@runDate AS DATE) -- Use CAST(@runDate AS DATE) for BigQuery date comparison
    )
    WHERE
      rn = 1
  ),
  scd_transform_stg AS (
    -- FULL OUTER JOIN handles Inserts, Updates, and Unchanged
    SELECT
      COALESCE(i.customer_id, a.customer_id) AS customer_id,
      -- Case 1: Brand new customer (Insert) OR Updated Customer (Insert new active row)
      -- These derive the "new" attribute values, prioritizing incoming if available.
      COALESCE(i.first_name, a.first_name) AS first_name_new,
      COALESCE(i.last_name, a.last_name) AS last_name_new,
      COALESCE(i.email_address, a.email_address) AS email_address_new,
      COALESCE(i.phone_number, a.phone_number) AS phone_number_new,
      COALESCE(i.residential_address, a.residential_address) AS residential_address_new,
      COALESCE(i.marital_status, a.marital_status) AS marital_status_new,

      -- Evaluate if a change actually occurred
      CASE
        WHEN a.customer_id IS NULL THEN 'INSERT' -- Brand New
        WHEN i.customer_id IS NOT NULL AND (
             COALESCE(i.last_name, '') != COALESCE(a.last_name, '') OR
             COALESCE(i.email_address, '') != COALESCE(a.email_address, '') OR
             COALESCE(i.residential_address, '') != COALESCE(a.residential_address, '') OR
             COALESCE(i.marital_status, '') != COALESCE(a.marital_status, '')
        ) THEN 'UPDATE'
        WHEN i.customer_id IS NULL THEN 'NO_CHANGE_NO_INCOMING' -- No update received today for this previously active customer
        ELSE 'NO_CHANGE_SAME_PAYLOAD' -- Payload arrived but tracked columns are identical
      END AS change_type,

      -- Retain old record details to age it out
      a.customer_surrogate_key AS old_sk,
      a.first_name AS first_name_old,
      a.last_name AS last_name_old,
      a.email_address AS email_address_old,
      a.phone_number AS phone_number_old,
      a.residential_address AS residential_address_old,
      a.marital_status AS marital_status_old,
      a.effective_start_date AS old_start_date
    FROM
      incoming_updates AS i
    FULL OUTER JOIN
      active_records AS a
      ON i.customer_id = a.customer_id
  )
-- SELECT * FROM scd_transform_stg -- For debugging or to materialize intermediate results


-- ==============================================================================
-- STEP 2: Insert the retired rows (closing old effective_end_date, is_active = false)
-- This SELECT part will be used in an INSERT statement.
-- ==============================================================================
INSERT INTO `your_project_id.your_dataset_id.dim_customers_scd2` (
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
    CAST(@runDate AS TIMESTAMP) AS effective_end_date, -- Use runDate for end date
    FALSE AS is_active
FROM
    scd_transform_stg
WHERE
    change_type = 'UPDATE';


-- ==============================================================================
-- STEP 3: Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
-- This SELECT part will be used in a second INSERT statement.
-- ==============================================================================
INSERT INTO `your_project_id.your_dataset_id.dim_customers_scd2` (
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
    GENERATE_UUID() AS customer_surrogate_key, -- BigQuery's UUID generation
    customer_id,
    first_name_new,
    last_name_new,
    email_address_new,
    phone_number_new,
    residential_address_new,
    marital_status_new,
    CAST(@runDate AS TIMESTAMP) AS effective_start_date, -- Use runDate for start date
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS effective_end_date,
    TRUE AS is_active
FROM
    scd_transform_stg
WHERE
    change_type IN ('INSERT', 'UPDATE');

-- Note: 'NO_CHANGE_NO_INCOMING' and 'NO_CHANGE_SAME_PAYLOAD' records
-- are implicitly handled as the target `dim_customers_scd2` is append-only for SCD Type 2.
-- Their existing active records are not touched by these INSERT statements.
```

**Key BigQuery Adaptations:**

*   `STORED AS ORC` is removed as it's not applicable to BigQuery tables.
*   `to_date(timestamp)` becomes `DATE(timestamp)` or `CAST(@runDate AS DATE)`.
*   `reflect("java.util.UUID", "randomUUID")` becomes `GENERATE_UUID()`.
*   Database/table paths are `project.dataset.table`.

---

### 2. C# Application

We'll create a .NET console application that uses the `Google.Cloud.BigQuery.V2` NuGet package.

**2.1. Project Setup:**

1.  Create a new .NET Console Application: `dotnet new console -n BigQuerySCDApp`
2.  Navigate into the project directory: `cd BigQuerySCDApp`
3.  Add the BigQuery NuGet package: `dotnet add package Google.Cloud.BigQuery.V2`
4.  Add a configuration package (optional, but good practice): `dotnet add package Microsoft.Extensions.Configuration.Json`

**2.2. `appsettings.json` (Configuration)**

Create an `appsettings.json` file in your project root to hold BigQuery details. Make sure to set `Copy to Output Directory` to `Copy if newer` for this file in your IDE.

```json
{
  "BigQuery": {
    "ProjectId": "your-gcp-project-id",
    "DatasetId": "your_dataset_id",
    "DimensionTableName": "dim_customers_scd2",
    "LandingTableName": "customer_updates"
  },
  "RunDate": "2024-01-01" // Example run date, change as needed or get dynamically
}
```
**Important**: Replace `your-gcp-project-id` and `your_dataset_id` with your actual GCP project ID and BigQuery dataset ID.

**2.3. `Program.cs` (C# Code)**

```csharp
using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;

namespace BigQuerySCDApp
{
    public class Program
    {
        private static IConfiguration _configuration;

        public static async Task Main(string[] args)
        {
            _configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Retrieve configuration values
            string projectId = _configuration["BigQuery:ProjectId"];
            string datasetId = _configuration["BigQuery:DatasetId"];
            string dimTableName = _configuration["BigQuery:DimensionTableName"];
            string landingTableName = _configuration["BigQuery:LandingTableName"];
            string runDateString = _configuration["RunDate"]; // Get run date from config

            if (string.IsNullOrEmpty(projectId) || string.IsNullOrEmpty(datasetId) ||
                string.IsNullOrEmpty(dimTableName) || string.IsNullOrEmpty(landingTableName) ||
                string.IsNullOrEmpty(runDateString))
            {
                Console.WriteLine("Error: BigQuery configuration or RunDate is missing in appsettings.json.");
                return;
            }

            // Parse run date
            if (!DateTime.TryParse(runDateString, out DateTime runDate))
            {
                Console.WriteLine($"Error: Invalid RunDate format in appsettings.json: {runDateString}");
                return;
            }

            Console.WriteLine($"Starting SCD Type 2 merge for customers on {runDate.ToShortDateString()}...");
            Console.WriteLine($"BigQuery Project: {projectId}, Dataset: {datasetId}");

            BigQueryClient client = BigQueryClient.Create(projectId);

            try
            {
                // Ensure the target table exists
                await CreateDimensionTableIfNotExists(client, datasetId, dimTableName);

                // Execute the transformation and merge
                await ExecuteSCDMerge(client, projectId, datasetId, dimTableName, landingTableName, runDate);

                Console.WriteLine("SCD Type 2 merge completed successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
                Console.WriteLine(ex.ToString());
            }
        }

        private static async Task CreateDimensionTableIfNotExists(
            BigQueryClient client, string datasetId, string tableName)
        {
            Console.WriteLine($"Checking/Creating table: {datasetId}.{tableName}");

            string createTableSql = $@"
                CREATE TABLE IF NOT EXISTS `{client.ProjectId}.{datasetId}.{tableName}` (
                    customer_surrogate_key STRING,
                    customer_id STRING,
                    first_name STRING,
                    last_name STRING,
                    email_address STRING,
                    phone_number STRING,
                    residential_address STRING,
                    marital_status STRING,
                    effective_start_date TIMESTAMP,
                    effective_end_date TIMESTAMP,
                    is_active BOOL
                );";

            Console.WriteLine("Executing Create Table DDL...");
            await client.ExecuteQueryAsync(createTableSql, parameters: null, queryJobOptions: null);
            Console.WriteLine("Create Table DDL execution finished.");
        }

        private static async Task ExecuteSCDMerge(
            BigQueryClient client,
            string projectId,
            string datasetId,
            string dimTableName,
            string landingTableName,
            DateTime runDate)
        {
            string runDateFormatted = runDate.ToString("yyyy-MM-dd");

            // Combine all the SQL logic into a single string for BigQuery.
            // BigQuery allows using CTEs for multiple statements within one query job.
            // We'll use a single query that defines the CTEs and then performs the inserts.
            string scdSql = $@"
WITH
  active_records AS (
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
    FROM
      `{projectId}.{datasetId}.{dimTableName}`
    WHERE
      is_active = TRUE
  ),
  incoming_updates AS (
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
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) AS rn
      FROM
        `{projectId}.{datasetId}.{landingTableName}`
      WHERE
        DATE(timestamp) = CAST(@runDate AS DATE)
    )
    WHERE
      rn = 1
  ),
  scd_transform_stg AS (
    SELECT
      COALESCE(i.customer_id, a.customer_id) AS customer_id,
      COALESCE(i.first_name, a.first_name) AS first_name_new,
      COALESCE(i.last_name, a.last_name) AS last_name_new,
      COALESCE(i.email_address, a.email_address) AS email_address_new,
      COALESCE(i.phone_number, a.phone_number) AS phone_number_new,
      COALESCE(i.residential_address, a.residential_address) AS residential_address_new,
      COALESCE(i.marital_status, a.marital_status) AS marital_status_new,

      CASE
        WHEN a.customer_id IS NULL THEN 'INSERT'
        WHEN i.customer_id IS NOT NULL AND (
             COALESCE(i.last_name, '') != COALESCE(a.last_name, '') OR
             COALESCE(i.email_address, '') != COALESCE(a.email_address, '') OR
             COALESCE(i.residential_address, '') != COALESCE(a.residential_address, '') OR
             COALESCE(i.marital_status, '') != COALESCE(a.marital_status, '')
        ) THEN 'UPDATE'
        WHEN i.customer_id IS NULL THEN 'NO_CHANGE_NO_INCOMING'
        ELSE 'NO_CHANGE_SAME_PAYLOAD'
      END AS change_type,

      a.customer_surrogate_key AS old_sk,
      a.first_name AS first_name_old,
      a.last_name AS last_name_old,
      a.email_address AS email_address_old,
      a.phone_number AS phone_number_old,
      a.residential_address AS residential_address_old,
      a.marital_status AS marital_status_old,
      a.effective_start_date AS old_start_date
    FROM
      incoming_updates AS i
    FULL OUTER JOIN
      active_records AS a
      ON i.customer_id = a.customer_id
  );

-- ==============================================================================
-- Insert the retired rows (closing old effective_end_date, is_active = false)
-- ==============================================================================
INSERT INTO `{projectId}.{datasetId}.{dimTableName}` (
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
    CAST(@runDate AS TIMESTAMP) AS effective_end_date,
    FALSE AS is_active
FROM
    scd_transform_stg
WHERE
    change_type = 'UPDATE';

-- ==============================================================================
-- Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
-- ==============================================================================
INSERT INTO `{projectId}.{datasetId}.{dimTableName}` (
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
    GENERATE_UUID() AS customer_surrogate_key,
    customer_id,
    first_name_new,
    last_name_new,
    email_address_new,
    phone_number_new,
    residential_address_new,
    marital_status_new,
    CAST(@runDate AS TIMESTAMP) AS effective_start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS effective_end_date,
    TRUE AS is_active
FROM
    scd_transform_stg
WHERE
    change_type IN ('INSERT', 'UPDATE');";

            Console.WriteLine("Executing SCD merge SQL job...");

            // BigQueryParameter to pass the runDate
            BigQueryParameter[] parameters = new[]
            {
                new BigQueryParameter("runDate", BigQueryDbType.Date, runDate)
            };

            await client.ExecuteQueryAsync(scdSql, parameters: parameters, queryJobOptions: null);

            Console.WriteLine("SCD merge SQL job execution finished.");
        }
    }
}
```

**To Run the C# Application:**

1.  **Authentication:**
    *   Make sure you have `GOOGLE_APPLICATION_CREDENTIALS` environment variable set to the path of your service account key JSON file.
    *   Alternatively, if running on GCP (Cloud Run, Cloud Functions, GKE, Compute Engine), default credentials will usually work automatically.
    *   You can also specify credentials explicitly in `BigQueryClient.Create()`, but using `GOOGLE_APPLICATION_CREDENTIALS` is generally recommended for local development.

2.  **`appsettings.json`:** Update `ProjectId`, `DatasetId`, and `RunDate` (e.g., "2024-01-01" or a more recent date for your test data).

3.  **BigQuery Landing Table (`customer_updates`):** Ensure you have a `customer_updates` table in your specified dataset with some sample data.
    ```sql
    CREATE TABLE `your_project_id.your_dataset_id.customer_updates` (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email_address STRING,
        phone_number STRING,
        residential_address STRING,
        marital_status STRING,
        timestamp TIMESTAMP
    );

    INSERT INTO `your_project_id.your_dataset_id.customer_updates` VALUES
    ('CUST001', 'John', 'Doe', 'john.doe@example.com', '111-222-3333', '123 Main St', 'Single', '2024-01-01 10:00:00'),
    ('CUST002', 'Jane', 'Smith', 'jane.smith@example.com', '444-555-6666', '456 Oak Ave', 'Married', '2024-01-01 11:00:00'),
    ('CUST001', 'John', 'Dorian', 'john.dorian@example.com', '111-222-3333', '789 Pine Ln', 'Divorced', '2024-01-01 12:00:00'), -- Update for CUST001
    ('CUST003', 'Alice', 'Wonderland', 'alice.w@example.com', '777-888-9999', 'Enchanted Forest', 'Single', '2024-01-01 13:00:00');
    ```

4.  **Run:** `dotnet run`

This setup provides a robust way to manage your SCD Type 2 logic within BigQuery, orchestrated by a C# application, fulfilling all the requirements of your conversion request.