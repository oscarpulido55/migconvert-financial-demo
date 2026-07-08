import com.google.cloud.bigquery.*;
import java.util.UUID; // Not strictly needed for basic query execution, but good for Job IDs if using async

public class FactDailyBalancesETL {

    // Replace with your Google Cloud Project ID
    private static final String PROJECT_ID = "your-gcp-project-id";
    // Dataset ID for financial core data
    private static final String DATASET_ID = "fin_core";
    // Parameter for the specific balance date being processed
    // In a real application, this would typically be passed dynamically (e.g., as a program argument)
    private static final String BALANCE_DATE_PARAM = "2024-01-01";

    public static void main(String[] args) {
        FactDailyBalancesETL etl = new FactDailyBalancesETL();
        try {
            etl.runDailyBalancesEtl();
            System.out.println("ETL job completed successfully for balance date: " + BALANCE_DATE_PARAM);
        } catch (BigQueryException e) {
            System.err.println("ETL job failed for balance date: " + BALANCE_DATE_PARAM + " - " + e.getMessage());
            e.printStackTrace();
            System.exit(1); // Indicate failure
        } catch (InterruptedException e) {
            System.err.println("ETL job interrupted for balance date: " + BALANCE_DATE_PARAM + " - " + e.getMessage());
            Thread.currentThread().interrupt(); // Restore the interrupted status
            e.printStackTrace();
            System.exit(1); // Indicate failure
        }
    }

    /**
     * Executes the daily financial balances ETL process.
     * @throws BigQueryException if a BigQuery operation fails.
     * @throws InterruptedException if the BigQuery query is interrupted.
     */
    public void runDailyBalancesEtl() throws BigQueryException, InterruptedException {
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();

        System.out.println("Step 1: Ensuring dataset and tables exist...");
        createDatasetAndTables(bigquery);

        System.out.println("Step 2: Processing daily movements into a temporary staging table...");
        processDailyMovements(bigquery);

        System.out.println("Step 3: Calculating and inserting/overwriting daily balances...");
        calculateAndInsertDailyBalances(bigquery);

        System.out.println("Step 4: Cleaning up temporary staging table...");
        cleanupTemporaryTable(bigquery);
    }

    /**
     * Helper method to execute a BigQuery SQL query.
     * @param bigquery The BigQuery client instance.
     * @param query The SQL query string to execute.
     * @throws BigQueryException if the query execution fails.
     * @throws InterruptedException if the query is interrupted.
     */
    private void executeQuery(BigQuery bigquery, String query) throws BigQueryException, InterruptedException {
        // For DDL/DML, setting a sufficiently long timeout is good practice.
        // Also, explicitly setting the JobId can help with monitoring in BigQuery UI.
        JobId jobId = JobId.newBuilder().setJob(UUID.randomUUID().toString()).setProject(PROJECT_ID).build();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
            .setUseLegacySql(false) // Use Standard SQL
            .build();

        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the job to complete.
        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new BigQueryException(BigQueryError.newBuilder("JOB_NOT_FOUND", null).setMessage("Job no longer exists").build(), "Job not found after execution.");
        } else if (queryJob.getStatus().getError() != null) {
            throw new BigQueryException(queryJob.getStatus().getError());
        }

        System.out.println("Query executed successfully: " + jobId.getJob());
    }

    /**
     * Creates the `fin_core` dataset and the `fact_daily_balances` table if they do not already exist.
     * @param bigquery The BigQuery client instance.
     * @throws BigQueryException if creation fails.
     * @throws InterruptedException if a query is interrupted.
     */
    private void createDatasetAndTables(BigQuery bigquery) throws BigQueryException, InterruptedException {
        // Create the dataset if it does not exist
        String createDatasetSql = String.format(
            "CREATE SCHEMA IF NOT EXISTS `%s`.%s;",
            PROJECT_ID,
            DATASET_ID
        );
        executeQuery(bigquery, createDatasetSql);

        // Create the fact_daily_balances table with partitioning and clustering
        String createFactDailyBalancesSql = String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.%s.fact_daily_balances (\n" +
            "    account_id STRING OPTIONS(description='Unique identifier for the account'),\n" +
            "    customer_id STRING OPTIONS(description='Identifier for the account owner'),\n" +
            "    account_type STRING OPTIONS(description='Type of account (CHECKING, SAVINGS, LOAN)'),\n" +
            "    open_date DATE OPTIONS(description='Date the account was opened'),\n" +
            "    currency_code STRING OPTIONS(description='Base currency of the account'),\n" +
            "    beginning_balance NUMERIC(18, 4) OPTIONS(description='Balance at the start of the day'),\n" +
            "    total_credits NUMERIC(18, 4) OPTIONS(description='Total value of incoming funds'),\n" +
            "    total_debits NUMERIC(18, 4) OPTIONS(description='Total value of outgoing funds'),\n" +
            "    ending_balance NUMERIC(18, 4) OPTIONS(description='Balance at the end of the day'),\n" +
            "    interest_accrued NUMERIC(18, 4) OPTIONS(description='Daily interest accrued'),\n" +
            "    is_overdrawn BOOL OPTIONS(description='Flag indicating if the account is in negative balance'),\n" +
            "    etl_timestamp TIMESTAMP OPTIONS(description='Timestamp of the ETL run'),\n" +
            "    balance_date DATE OPTIONS(description='Date for which the balance is calculated'),\n" +
            "    region_code STRING OPTIONS(description='Region code for the account')\n" +
            ")\n" +
            "PARTITION BY balance_date\n" +
            "CLUSTER BY region_code\n" +
            "OPTIONS(\n" +
            "    description='Stores the End of Day balances for all accounts'\n" +
            ");",
            PROJECT_ID,
            DATASET_ID
        );
        executeQuery(bigquery, createFactDailyBalancesSql);
    }

    /**
     * Creates a temporary staging table with today's aggregated transaction movements.
     * @param bigquery The BigQuery client instance.
     * @throws BigQueryException if table creation fails.
     * @throws InterruptedException if a query is interrupted.
     */
    private void processDailyMovements(BigQuery bigquery) throws BigQueryException, InterruptedException {
        // Drop the temporary table if it already exists from a previous failed run
        String dropTempTableSql = String.format(
            "DROP TABLE IF EXISTS `%s`.%s.tmp_daily_movements_stg;",
            PROJECT_ID,
            DATASET_ID
        );
        executeQuery(bigquery, dropTempTableSql);

        // Create the temporary table with aggregated daily movements
        String createTempTableSql = String.format(
            "CREATE TABLE `%s`.%s.tmp_daily_movements_stg AS\n" +
            "WITH credited AS (\n" +
            "    SELECT \n" +
            "        destination_account_id AS account_id,\n" +
            "        SUM(amount_base_currency) AS total_credits\n" +
            "    FROM `%s`.%s.fact_transactions\n" +
            "    WHERE trx_date = '%s'\n" +
            "      AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')\n" +
            "    GROUP BY destination_account_id\n" +
            "),\n" +
            "debited AS (\n" +
            "    SELECT \n" +
            "        source_account_id AS account_id,\n" +
            "        SUM(amount_base_currency) AS total_debits\n" +
            "    FROM `%s`.%s.fact_transactions\n" +
            "    WHERE trx_date = '%s'\n" +
            "      AND transaction_type NOT IN ('REVERSAL_CREDIT')\n" +
            "    GROUP BY source_account_id\n" +
            ")\n" +
            "SELECT \n" +
            "    COALESCE(c.account_id, d.account_id) AS account_id,\n" +
            "    COALESCE(c.total_credits, 0.0) AS total_credits,\n" +
            "    COALESCE(d.total_debits, 0.0) AS total_debits\n" +
            "FROM credited c\n" +
            "FULL OUTER JOIN debited d ON c.account_id = d.account_id;",
            PROJECT_ID,
            DATASET_ID,
            PROJECT_ID,
            DATASET_ID,
            BALANCE_DATE_PARAM,
            PROJECT_ID,
            DATASET_ID,
            BALANCE_DATE_PARAM
        );
        executeQuery(bigquery, createTempTableSql);
    }

    /**
     * Calculates the end-of-day balances and inserts/overwrites them into the `fact_daily_balances` table.
     * This achieves 'overwrite partition' semantics by first deleting data for the target date.
     * @param bigquery The BigQuery client instance.
     * @throws BigQueryException if insert/delete fails.
     * @throws InterruptedException if a query is interrupted.
     */
    private void calculateAndInsertDailyBalances(BigQuery bigquery) throws BigQueryException, InterruptedException {
        // Implement INSERT OVERWRITE semantics for the specific partition by first deleting existing data for the BALANCE_DATE_PARAM
        String deleteExistingPartitionSql = String.format(
            "DELETE FROM `%s`.%s.fact_daily_balances\n" +
            "WHERE balance_date = DATE '%s';",
            PROJECT_ID,
            DATASET_ID,
            BALANCE_DATE_PARAM
        );
        executeQuery(bigquery, deleteExistingPartitionSql);

        // Insert new/updated daily balances
        String insertBalancesSql = String.format(
            "INSERT INTO `%s`.%s.fact_daily_balances (\n" +
            "    account_id, customer_id, account_type, open_date, currency_code,\n" +
            "    beginning_balance, total_credits, total_debits, ending_balance,\n" +
            "    interest_accrued, is_overdrawn, etl_timestamp, balance_date, region_code\n" +
            ")\n" +
            "SELECT \n" +
            "    a.account_id,\n" +
            "    a.customer_id,\n" +
            "    a.account_type,\n" +
            "    a.open_date,\n" +
            "    a.currency_code,\n" +
            "    COALESCE(prev.ending_balance, 0.0) AS beginning_balance,\n" +
            "    COALESCE(m.total_credits, 0.0) AS total_credits,\n" +
            "    COALESCE(m.total_debits, 0.0) AS total_debits,\n" +
            "    (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,\n" +
            "    CASE \n" +
            "        WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0 \n" +
            "        THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)\n" +
            "        ELSE 0.0 \n" +
            "    END AS interest_accrued,\n" +
            "    CASE \n" +
            "        WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0 \n" +
            "        THEN TRUE \n" +
            "        ELSE FALSE \n" +
            "    END AS is_overdrawn,\n" +
            "    CURRENT_TIMESTAMP() AS etl_timestamp,\n" +
            "    DATE '%s' AS balance_date,\n" +
            "    COALESCE(a.region_code, 'UN') AS region_code\n" +
            "FROM `%s`.%s.dim_accounts a\n" +
            "LEFT JOIN `%s`.%s.fact_daily_balances prev \n" +
            "    ON a.account_id = prev.account_id \n" +
            "    AND prev.balance_date = DATE_SUB(DATE '%s', INTERVAL 1 DAY)\n" +
            "LEFT JOIN `%s`.%s.tmp_daily_movements_stg m \n" +
            "    ON a.account_id = m.account_id\n" +
            "WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');",
            PROJECT_ID,
            DATASET_ID,
            BALANCE_DATE_PARAM, // For balance_date
            PROJECT_ID,
            DATASET_ID, // For dim_accounts
            PROJECT_ID,
            DATASET_ID, // For fact_daily_balances prev
            BALANCE_DATE_PARAM, // For DATE_SUB function
            PROJECT_ID,
            DATASET_ID // For tmp_daily_movements_stg
        );
        executeQuery(bigquery, insertBalancesSql);
    }

    /**
     * Drops the temporary staging table after the ETL process is complete.
     * @param bigquery The BigQuery client instance.
     * @throws BigQueryException if table drop fails.
     * @throws InterruptedException if a query is interrupted.
     */
    private void cleanupTemporaryTable(BigQuery bigquery) throws BigQueryException, InterruptedException {
        String dropTempTableSql = String.format(
            "DROP TABLE IF EXISTS `%s`.%s.tmp_daily_movements_stg;",
            PROJECT_ID,
            DATASET_ID
        );
        executeQuery(bigquery, dropTempTableSql);
    }
}
