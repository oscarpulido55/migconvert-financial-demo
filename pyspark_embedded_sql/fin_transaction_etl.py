import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.JobInfo.Builder;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@code FinTransactionETL} class is responsible for executing an Extract, Transform, Load (ETL)
 * pipeline for financial transactions into Google BigQuery.
 *
 * This class translates a PySpark/Hive-based ETL process to a Java application
 * utilizing the Google BigQuery SDK. It handles data ingestion from a landing zone,
 * transformation into a core fact table, and aggregation into a risk analysis mart.
 *
 * Key features include:
 * - Initialization of the BigQuery client.
 * - Ensuring existence of required datasets (schemas in BigQuery terminology).
 * - Executing complex SQL queries, including temporary table creation, window functions,
 *   and conditional logic, directly against BigQuery.
 * - Implementing 'INSERT OVERWRITE PARTITION' semantics by using a DELETE-then-INSERT
 *   strategy for date-partitioned tables.
 * - Utilizing BigQuery query parameters for safe and efficient date filtering.
 */
public class FinTransactionETL {

    /**
     * Logger instance for capturing and reporting application events and errors.
     */
    private static final Logger LOGGER = Logger.getLogger(FinTransactionETL.class.getName());

    /**
     * The Google Cloud Project ID where BigQuery datasets and tables reside.
     * This value should be replaced with your actual GCP Project ID or sourced from
     * a configuration management system or environment variables for production readiness.
     */
    private static final String PROJECT_ID = "your-gcp-project-id"; // IMPORTANT: Replace with your actual GCP Project ID

    /**
     * Initializes the BigQuery client.
     * This method constructs a BigQuery client using the specified project ID.
     * It relies on Application Default Credentials for authentication, which is
     * a robust and flexible approach for environments like GCP VMs, Cloud Functions,
     * or local development with authenticated gcloud CLI.
     *
     * @return An initialized {@link BigQuery} client instance.
     */
    private static BigQuery createBigQueryClient() {
        LOGGER.info("Initializing BigQuery client...");
        return BigQueryOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    }

    /**
     * Ensures that the necessary BigQuery datasets (`fin_landing`, `fin_core`, `fin_mart`) exist.
     * If a dataset does not exist within the specified {@code PROJECT_ID}, it will be created.
     * In a robust production environment, dataset provisioning is often managed through
     * Infrastructure as Code (IaC) tools like Terraform rather than directly within application logic.
     *
     * @param bigquery The {@link BigQuery} client instance used for dataset operations.
     * @throws RuntimeException If a dataset creation fails due to a {@link BigQueryException}.
     */
    private static void initializeDatasets(BigQuery bigquery) {
        String[] datasets = {"fin_landing", "fin_core", "fin_mart"};
        for (String datasetName : datasets) {
            DatasetId datasetId = DatasetId.of(PROJECT_ID, datasetName);
            Dataset dataset = bigquery.getDataset(datasetId); // Attempt to retrieve the dataset
            if (dataset == null) {
                LOGGER.log(Level.INFO, "Dataset ''{0}'' does not exist. Attempting to create...", datasetName);
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).build();
                try {
                    bigquery.create(datasetInfo); // Create the dataset if it doesn't exist
                    LOGGER.log(Level.INFO, "Dataset ''{0}'' created successfully.", datasetName);
                } catch (BigQueryException e) {
                    LOGGER.log(Level.SEVERE, "Failed to create dataset ''{0}'': {1}",
                            new Object[]{datasetName, e.getMessage()});
                    // Propagate the exception as a RuntimeException to halt execution
                    throw new RuntimeException("Failed to create dataset " + datasetName, e);
                }
            } else {
                LOGGER.log(Level.INFO, "Dataset ''{0}'' already exists.", datasetName);
            }
        }
    }

    /**
     * Executes the main ETL pipeline for financial transactions.
     * This method orchestrates a series of BigQuery SQL operations:
     * 1. Creates a temporary table for raw transactions filtered by the processing date.
     * 2. Loads and transforms data into the {@code fin_core.fact_transactions} table.
     *    It employs a DELETE-then-INSERT strategy to emulate 'INSERT OVERWRITE TABLE PARTITION'
     *    behavior for the target {@code trx_date}.
     * 3. Aggregates data from the fact table into {@code fin_mart.risk_daily_summary}
     *    for daily risk analysis, also using a DELETE-then-INSERT for the {@code summary_date}.
     *
     * @param bigquery The {@link BigQuery} client instance used to execute queries.
     * @param processDate The date string (format YYYY-MM-DD) for which the ETL pipeline
     *                    should process data. This is passed as a query parameter.
     * @throws InterruptedException If any BigQuery job is interrupted while waiting for completion.
     * @throws BigQueryException If a BigQuery-specific error occurs during query execution or job status check.
     */
    private static void runEtlPipeline(BigQuery bigquery, String processDate)
            throws InterruptedException, BigQueryException {
        LOGGER.log(Level.INFO, "Starting ETL pipeline execution for process_date: {0}", processDate);

        // Define a query parameter for the process date to prevent SQL injection and for type safety.
        QueryParameterValue processDateParam = QueryParameterValue.date(processDate);

        // Step 1: Create a temporary table (`raw_trx_delta`) to isolate raw transactions for the specific process date.
        // This temporary table acts as a staging area for the day's raw transaction data,
        // making subsequent joins and transformations cleaner and potentially more efficient.
        LOGGER.info("Creating temporary table for raw transactions: raw_trx_delta...");
        String createRawTrxDeltaTempTableSql = String.format("""
            CREATE TEMPORARY TABLE raw_trx_delta AS
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
            FROM `%s.fin_landing.raw_transactions`
            WHERE CAST(transaction_timestamp AS DATE) = @process_date
            """, PROJECT_ID);

        // Configure and execute the job to create the temporary table.
        QueryJobConfiguration createTempTableConfig = QueryJobConfiguration.newBuilder(createRawTrxDeltaTempTableSql)
                .addNamedParameter("process_date", processDateParam)
                .build();
        executeBigQueryJob(bigquery, createTempTableConfig, "Create raw_trx_delta temporary table");

        // Step 2: Load and transform data into the core fact table (`fin_core.fact_transactions`).
        // This process involves enriching raw transaction data with customer and account dimensions,
        // calculating normalized amounts, and computing rolling sums for analytical purposes.
        // It uses a DELETE-then-INSERT pattern to achieve Hive's `INSERT OVERWRITE TABLE PARTITION` semantics
        // for the `trx_date` partition.
        LOGGER.info("Processing data for fin_core.fact_transactions (DELETE then INSERT for overwrite)...");

        // First, delete any existing fact transactions for the current process date.
        // This ensures that the daily load is idempotent and overwrites previous runs for the same date.
        String deleteFactTransactionsSql = String.format("""
            DELETE FROM `%s.fin_core.fact_transactions`
            WHERE trx_date = @process_date
            """, PROJECT_ID);
        QueryJobConfiguration deleteFactConfig = QueryJobConfiguration.newBuilder(deleteFactTransactionsSql)
                .addNamedParameter("process_date", processDateParam)
                .build();
        executeBigQueryJob(bigquery, deleteFactConfig, "Delete existing fact transactions for process_date");

        // Then, insert the newly processed and transformed data for the current process date.
        String insertFactTransactionsSql = String.format("""
            INSERT INTO `%s.fin_core.fact_transactions` (
                trx_uuid, source_account_id, destination_account_id, transaction_type,
                amount_base_currency, normalized_usd_amount, currency_code, transaction_timestamp,
                merchant_category_code, channel, customer_id, customer_segment, kyc_status,
                rolling_10_trx_amount, status, trx_date, region_id
            )
            SELECT
                r.trx_uuid,
                r.source_account_id,
                r.destination_account_id,
                r.transaction_type,
                r.amount_base_currency,
                -- Calculate the normalized amount in USD, accounting for potential missing exchange rates.
                CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC) AS normalized_usd_amount,
                r.currency_code,
                r.transaction_timestamp,
                r.merchant_category_code,
                r.channel,
                c.customer_id,
                c.customer_segment,
                c.kyc_status,
                -- Compute a rolling sum of transaction amounts for the last 10 transactions
                -- for each source account, useful for anomaly detection.
                SUM(r.amount_base_currency) OVER (
                    PARTITION BY r.source_account_id
                    ORDER BY r.transaction_timestamp
                    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                ) AS rolling_10_trx_amount,
                r.status,
                -- Derive partitioning columns: `trx_date` from transaction timestamp and `region_id` from dim_accounts.
                CAST(r.transaction_timestamp AS DATE) AS trx_date,
                COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
            FROM raw_trx_delta AS r
            LEFT JOIN `%s.fin_core.dim_accounts` AS dim_a
                ON r.source_account_id = dim_a.account_id
            LEFT JOIN `%s.fin_core.dim_customers` AS c
                ON dim_a.customer_id = c.customer_id
            WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE') -- Filter for relevant transaction statuses.
              AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL' -- Exclude specific transaction types.
            """, PROJECT_ID, PROJECT_ID, PROJECT_ID);

        // Configure and execute the job to insert data into the fact table.
        QueryJobConfiguration insertFactConfig = QueryJobConfiguration.newBuilder(insertFactTransactionsSql)
                .addNamedParameter("process_date", processDateParam) // Still relevant for raw_trx_delta source
                .build();
        executeBigQueryJob(bigquery, insertFactConfig, "Insert into fin_core.fact_transactions");

        // Step 3: Create an aggregated datamart for daily risk analysis (`fin_mart.risk_daily_summary`).
        // This aggregates fact table data to provide key metrics and a risk flag per customer,
        // enabling quick identification of potentially suspicious activities.
        LOGGER.info("Executing aggregation for Risk Datamart (fin_mart.risk_daily_summary) (DELETE then INSERT for overwrite)...");

        // First, delete any existing daily risk summaries for the current process date.
        // This ensures the summary is rebuilt daily and is idempotent.
        String deleteRiskSummarySql = String.format("""
            DELETE FROM `%s.fin_mart.risk_daily_summary`
            WHERE summary_date = @process_date
            """, PROJECT_ID);
        QueryJobConfiguration deleteRiskConfig = QueryJobConfiguration.newBuilder(deleteRiskSummarySql)
                .addNamedParameter("process_date", processDateParam)
                .build();
        executeBigQueryJob(bigquery, deleteRiskConfig, "Delete existing risk summary for process_date");

        // Then, insert the newly calculated daily risk summary based on the fact table data.
        String insertRiskSummarySql = String.format("""
            INSERT INTO `%s.fin_mart.risk_daily_summary` (
                customer_id, customer_segment, region_id, total_daily_transactions,
                total_daily_volume_usd, max_single_transaction_usd, high_risk_mcc_count,
                unique_destinations_count, daily_risk_flag, summary_date
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
                -- A conditional flag for immediate review based on transaction volume and customer segment.
                CASE
                    WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                    WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS daily_risk_flag,
                @process_date AS summary_date -- Set the summary date from the input parameter.
            FROM `%s.fin_core.fact_transactions`
            WHERE trx_date = @process_date -- Filter for data relevant to the current process date.
            GROUP BY
                customer_id,
                customer_segment,
                region_id
            """, PROJECT_ID, PROJECT_ID);

        // Configure and execute the job to insert data into the risk summary mart.
        QueryJobConfiguration insertRiskConfig = QueryJobConfiguration.newBuilder(insertRiskSummarySql)
                .addNamedParameter("process_date", processDateParam)
                .build();
        executeBigQueryJob(bigquery, insertRiskConfig, "Insert into fin_mart.risk_daily_summary");

        LOGGER.info("Successfully completed ETL pipeline.");
    }

    /**
     * Executes a BigQuery SQL query job and waits for its completion.
     * This utility method simplifies query execution and robust error handling.
     * It logs the job's progress and status, throwing an exception if the job fails.
     *
     * @param bigquery The {@link BigQuery} client.
     * @param config The {@link QueryJobConfiguration} specifying the query and its parameters.
     * @param jobDescription A descriptive string for logging purposes, identifying the job's purpose.
     * @throws InterruptedException If the job execution thread is interrupted while waiting.
     * @throws BigQueryException If BigQuery reports an error for the executed job.
     */
    private static void executeBigQueryJob(BigQuery bigquery, QueryJobConfiguration config, String jobDescription)
            throws InterruptedException, BigQueryException {
        LOGGER.log(Level.INFO, "Executing BigQuery job: {0}", jobDescription);

        // Build the JobInfo and create the job in BigQuery.
        // It's possible to add advanced options like table definitions or destination table here.
        Builder jobBuilder = JobInfo.newBuilder(config);
        Job job = bigquery.create(jobBuilder.build());

        // Wait for the job to complete. This is a synchronous call.
        job = job.waitFor();

        // Check job status and report.
        if (job.isDone() && job.getStatus().getError() == null) {
            LOGGER.log(Level.INFO, "BigQuery job ''{0}'' completed successfully. Job ID: {1}",
                    new Object[]{jobDescription, job.getJobId().getJob()});
        } else if (job.getStatus().getError() != null) {
            String errorMessage = job.getStatus().getError().getMessage();
            LOGGER.log(Level.SEVERE, "BigQuery job ''{0}'' failed. Job ID: {1}, Error: {2}",
                    new Object[]{jobDescription, job.getJobId().getJob(), errorMessage});
            // Propagate the BigQuery error as an exception.
            throw new BigQueryException(BigQueryError.newBuilder(null, null, errorMessage).build());
        } else {
            // This case indicates an unexpected state where the job is not done and has no error.
            // Log as warning and potentially re-throw or handle based on business logic.
            LOGGER.log(Level.WARNING, "BigQuery job ''{0}'' status unknown. Job ID: {1}",
                    new Object[]{jobDescription, job.getJobId().getJob()});
        }
    }

    /**
     * Main entry point for the Financial Transaction ETL application.
     * This method parses command-line arguments, initializes the BigQuery client,
     * ensures required datasets are in place, and then executes the ETL pipeline.
     * It requires a single argument: the processing date in YYYY-MM-DD format.
     *
     * @param args Command-line arguments. Expects one argument: the process date (e.g., "2023-10-26").
     */
    public static void main(String[] args) {
        // Validate command-line arguments.
        if (args.length != 1) {
            System.out.println("Usage: java FinTransactionETL <YYYY-MM-DD>");
            System.exit(1); // Exit with error code if arguments are incorrect.
        }

        String processDate = args[0]; // Extract the process date from arguments.
        BigQuery bigquery = null; // Initialize BigQuery client as null, to be instantiated in try block.

        try {
            // Step 1: Initialize the BigQuery client.
            bigquery = createBigQueryClient();

            // Step 2: Ensure all necessary BigQuery datasets exist.
            initializeDatasets(bigquery);

            // Step 3: Run the core ETL pipeline for the specified processing date.
            runEtlPipeline(bigquery, processDate);

        } catch (BigQueryException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "ETL pipeline failed for process_date ''{0}'': {1}",
                    new Object[]{processDate, e.getMessage()});
            LOGGER.log(Level.SEVERE, "Detailed exception:", e); // Log full stack trace for debugging.
            System.exit(1); // Exit with error code on pipeline failure.
        } finally {
            // BigQuery client instances generally do not require explicit closing
            // as they manage their own resources and connections.
            LOGGER.info("ETL application finished.");
        }
    }
}
