```java
// Ensure you have the Google Cloud BigQuery client library in your project's dependencies (e.g., in pom.xml for Maven):
/*
<dependency>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-bigquery</artifactId>
    <version>2.33.0</version> <!-- Use the latest stable version -->
</dependency>
<dependency>
    <groupId>com.google.cloud</groupId>
    <artifactId>google-cloud-core</artifactId>
    <version>2.33.0</version> <!-- Match with bigquery version -->
</dependency>
*/

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.QueryParameterValue;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@code FinancialTransactionETL} class performs an Extract, Transform, Load (ETL) process
 * for financial transactions. It processes raw transaction data, populates a fact table,
 * and generates a daily risk summary using Google BigQuery.
 *
 * <p>This class replaces a PySpark/Hive-based ETL pipeline, adapting the logic and
 * SQL queries to work with the Google BigQuery SDK.
 */
public class FinancialTransactionETL {

    // Initialize a logger for the class.
    private static final Logger logger = Logger.getLogger(FinancialTransactionETL.class.getName());

    // BigQuery client instance.
    private final BigQuery bigquery;

    /**
     * Constructs a new {@code FinancialTransactionETL} instance.
     * Initializes the Google BigQuery client. It uses default credentials,
     * which might be a service account key, the {@code GOOGLE_APPLICATION_CREDENTIALS} environment variable,
     * or a GCE/Cloud Run default service account.
     */
    public FinancialTransactionETL() {
        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        logger.log(Level.INFO, "BigQuery client initialized.");
    }

    /**
     * Ensures that necessary BigQuery datasets (schemas) exist within the current project.
     * In BigQuery, datasets are logical containers for tables, analogous to databases in Hive.
     * If a dataset does not exist, it will be created.
     *
     * @param projectId The Google Cloud Project ID where the datasets should reside.
     * @throws RuntimeException if any BigQuery dataset operation fails.
     */
    private void ensureBigQueryDatasets(String projectId) {
        // List of datasets required for the ETL process.
        String[] datasets = {"fin_landing", "fin_core", "fin_mart"};
        for (String datasetName : datasets) {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            try {
                // Check if the dataset already exists.
                if (bigquery.getDataset(datasetId) == null) {
                    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).build();
                    bigquery.create(datasetInfo);
                    logger.log(Level.INFO, "BigQuery dataset ''{0}'' created successfully.", datasetName);
                } else {
                    logger.log(Level.INFO, "BigQuery dataset ''{0}'' already exists.", datasetName);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to create or check BigQuery dataset ''{0}''.", datasetName);
                throw new RuntimeException("Dataset operation failed for " + datasetName, e);
            }
        }
    }

    /**
     * Executes a BigQuery SQL query with named parameters.
     * This method handles the execution of DML and DDL statements, waits for job completion,
     * and reports on job status.
     *
     * @param querySql The SQL query string to execute. BigQuery's standard SQL is used.
     * @param jobIdPrefix An optional prefix for the BigQuery job ID to help with tracking and debugging.
     * @param namedParameters A map of named parameters (e.g., "@paramName") to their values. Can be null or empty.
     * @return The {@link TableResult} from the query execution (may be empty for DML statements).
     * @throws InterruptedException If the query job is interrupted while waiting for completion.
     * @throws RuntimeException If the BigQuery job fails or encounters an error.
     */
    private TableResult executeQuery(String querySql, String jobIdPrefix, Map<String, QueryParameterValue> namedParameters) throws InterruptedException {
        // Generate a unique job ID to ensure idempotency and distinct job tracking.
        String jobIdString = (jobIdPrefix != null ? jobIdPrefix + "-" : "") + UUID.randomUUID().toString();
        JobId jobId = JobId.of(bigquery.getOptions().getProjectId(), jobIdString);

        QueryJobConfiguration.Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(querySql);

        // Apply named parameters if provided. BigQuery prefers named parameters for clarity and safety.
        if (namedParameters != null && !namedParameters.isEmpty()) {
            queryConfigBuilder.setNamedParameters(namedParameters);
        }

        // Set to use Standard SQL (not Legacy SQL).
        QueryJobConfiguration queryConfig = queryConfigBuilder.setUseLegacySql(false).build();
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        logger.log(Level.INFO, "Starting BigQuery job ''{0}''...", jobId.getJob());

        // Wait for the query job to complete.
        queryJob = queryJob.waitFor();

        // Check for job existence and status.
        if (queryJob == null) {
            throw new RuntimeException("BigQuery job ''" + jobId + "'' no longer exists or was aborted.");
        } else if (queryJob.getStatus().getError() != null) {
            // Log execution errors if available for more detailed debugging.
            logger.log(Level.SEVERE, "BigQuery job ''{0}'' failed: {1}", new Object[]{jobId.getJob(), queryJob.getStatus().getError()});
            throw new RuntimeException("BigQuery job failed: " + queryJob.getStatus().getError().toString());
        }

        // Log completion details, specifically DML affected rows for INSERT/UPDATE/DELETE.
        Long affectedRows = (queryJob.getStatistics() != null && queryJob.getStatistics().getQueryStatistics() != null)
                ? queryJob.getStatistics().getQueryStatistics().getNumDmlAffectedRows()
                : 0L; // Default to 0 if statistics not available or not a DML job.
        logger.log(Level.INFO, "BigQuery job ''{0}'' completed. DML affected rows: {1}", new Object[]{jobId.getJob(), affectedRows});

        return queryJob.getQueryResults();
    }


    /**
     * Executes the ETL pipeline for financial transactions.
     * This method orchestrates the loading of raw data, transformation into a fact table,
     * and aggregation into a daily risk summary using BigQuery SQL queries.
     *
     * <p>It replaces Hive's `INSERT OVERWRITE TABLE PARTITION` semantic by explicitly performing
     * a {@code DELETE} for the specific process date partitions, followed by an {@code INSERT}
     * of new data. Temporary views are simulated using Common Table Expressions (CTEs) within queries.
     *
     * @param processDate The date for which to process transactions, in YYYY-MM-DD format.
     * @throws RuntimeException If any BigQuery operation fails or the process date is invalid.
     */
    public void runEtlPipeline(String processDate) {
        logger.log(Level.INFO, "Starting ETL pipeline execution for process_date: {0}", processDate);

        // Validate and parse the process_date into LocalDate.
        LocalDate pDate;
        try {
            pDate = LocalDate.parse(processDate);
        } catch (DateTimeParseException e) {
            logger.log(Level.SEVERE, "Invalid process date format: {0}. Expected YYYY-MM-DD.", processDate);
            throw new RuntimeException("Invalid process date format.", e);
        }

        // Define a map for named query parameters, ensuring safe and correct date substitution.
        Map<String, QueryParameterValue> params = new HashMap<>();
        params.put("process_date", QueryParameterValue.date(pDate.toString()));

        String projectId = bigquery.getOptions().getProjectId();
        if (projectId == null) {
            throw new RuntimeException("Google Cloud Project ID is null. Cannot form fully qualified table names.");
        }

        // --- ETL Step 1 & 2: Process Raw Transactions and Populate fin_core.fact_transactions ---
        // The original `CREATE OR REPLACE TEMPORARY VIEW raw_trx_delta` is integrated as a CTE
        // within the main `INSERT` statement for `fact_transactions`. This is a common and efficient
        // pattern in BigQuery.
        // Hive's `INSERT OVERWRITE TABLE PARTITION` is mimicked by a DELETE for the target date's data,
        // followed by an INSERT of the newly processed data.

        logger.log(Level.INFO, "Processing raw transactions and inserting into fin_core.fact_transactions for process_date: {0}", processDate);

        // Delete existing data for the process date from `fact_transactions`.
        String deleteFactTransactionsSql = String.format("""
            DELETE FROM `%s.fin_core.fact_transactions`
            WHERE trx_date = @process_date
            """,
            projectId
        );

        // Insert new data into `fact_transactions`.
        // The `raw_trx_delta` CTE isolates the raw data selection for the given date.
        String insertFactTransactionsSql = String.format("""
            INSERT INTO `%s.fin_core.fact_transactions` (
                trx_uuid, source_account_id, destination_account_id, transaction_type,
                amount_base_currency, normalized_usd_amount, currency_code, transaction_timestamp,
                merchant_category_code, channel, customer_id, customer_segment, kyc_status,
                rolling_10_trx_amount, status, trx_date, region_id
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
                FROM `%s.fin_landing.raw_transactions`
                WHERE DATE(transaction_timestamp) = @process_date
            )
            SELECT
                r.trx_uuid,
                r.source_account_id,
                r.destination_account_id,
                r.transaction_type,
                r.amount_base_currency,
                -- Calculate normalized amount for aggregations, using BigQuery's NUMERIC type
                CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC) AS normalized_usd_amount,
                r.currency_code,
                r.transaction_timestamp,
                r.merchant_category_code,
                r.channel,
                c.customer_id,
                c.customer_segment,
                c.kyc_status,
                -- Rolling sum to flag consecutive large transactions using window functions
                SUM(r.amount_base_currency) OVER (
                    PARTITION BY r.source_account_id
                    ORDER BY r.transaction_timestamp
                    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
                ) AS rolling_10_trx_amount,
                r.status,
                -- Partitioning Columns: Convert timestamp to DATE type
                CAST(r.transaction_timestamp AS DATE) AS trx_date,
                COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
            FROM raw_trx_delta AS r
            LEFT JOIN `%s.fin_core.dim_accounts` AS dim_a ON r.source_account_id = dim_a.account_id
            LEFT JOIN `%s.fin_core.dim_customers` AS c ON dim_a.customer_id = c.customer_id
            WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
              AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
            """,
            projectId, // For the main INSERT destination table
            projectId, // For raw_transactions source
            projectId, // For dim_accounts source
            projectId  // For dim_customers source
        );

        try {
            // Execute the DELETE statement first.
            logger.log(Level.INFO, "Executing DELETE statement for fin_core.fact_transactions for date: {0}", processDate);
            executeQuery(deleteFactTransactionsSql, "deleteFactTrx", params);

            // Then execute the INSERT statement.
            logger.log(Level.INFO, "Executing INSERT statement for fin_core.fact_transactions.");
            executeQuery(insertFactTransactionsSql, "insertFactTrx", params);
        } catch (InterruptedException e) {
            // Restore the interrupted status and propagate as a RuntimeException.
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, "ETL pipeline interrupted during fact transactions processing.", e);
            throw new RuntimeException("ETL pipeline interrupted.", e);
        } catch (Exception e) {
            // Catch any other exceptions during fact table processing.
            logger.log(Level.SEVERE, "Failed to process fin_core.fact_transactions.", e);
            throw new RuntimeException("Failed to process fact transactions.", e);
        }


        // --- ETL Step 3: Create Aggregated Datamart for Risk Analysis ---
        // This step follows a similar DELETE + INSERT pattern for partitioned tables as in Step 2.
        logger.log(Level.INFO, "Executing aggregation for fin_mart.risk_daily_summary for process_date: {0}", processDate);

        // Delete existing risk summary data for the process date.
        String deleteRiskSummarySql = String.format("""
            DELETE FROM `%s.fin_mart.risk_daily_summary`
            WHERE summary_date = @process_date
            """,
            projectId
        );

        // Insert new aggregated risk summary data.
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
                -- Flag for Review based on risk criteria
                CASE
                    WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                    WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS daily_risk_flag,
                @process_date AS summary_date
            FROM `%s.fin_core.fact_transactions`
            WHERE trx_date = @process_date
            GROUP BY
                customer_id,
                customer_segment,
                region_id
            """,
            projectId, // For the main INSERT destination table
            projectId  // For fact_transactions source
        );

        try {
            // Execute the DELETE statement first.
            logger.log(Level.INFO, "Executing DELETE statement for fin_mart.risk_daily_summary for date: {0}", processDate);
            executeQuery(deleteRiskSummarySql, "deleteRiskSummary", params);

            // Then execute the INSERT statement.
            logger.log(Level.INFO, "Executing INSERT statement for fin_mart.risk_daily_summary.");
            executeQuery(insertRiskSummarySql, "insertRiskSummary", params);
        } catch (InterruptedException e) {
            // Restore the interrupted status and propagate as a RuntimeException.
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, "ETL pipeline interrupted during risk summary processing.", e);
            throw new RuntimeException("ETL pipeline interrupted.", e);
        } catch (Exception e) {
            // Catch any other exceptions during risk summary processing.
            logger.log(Level.SEVERE, "Failed to process fin_mart.risk_daily_summary.", e);
            throw new RuntimeException("Failed to process risk summary.", e);
        }

        logger.log(Level.INFO, "Successfully completed ETL pipeline for process_date: {0}", processDate);
    }

    /**
     * Main entry point for the Financial Transaction ETL application.
     * Expects a single command-line argument: the process date in YYYY-MM-DD format.
     *
     * <p>Configures basic logging, initializes the ETL process, ensures BigQuery datasets exist,
     * and then executes the ETL pipeline.
     *
     * @param args Command-line arguments. Expects one argument: {@code <YYYY-MM-DD>} process_date.
     */
    public static void main(String[] args) {
        // Configure Java's default logger format for better readability on console.
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$s %2$s %5$s%6$s%n");
        logger.setLevel(Level.INFO); // Set the default logging level for the application.

        // Validate command-line arguments.
        if (args.length != 1) {
            logger.log(Level.SEVERE, "Usage: java FinancialTransactionETL <YYYY-MM-DD>");
            System.exit(1); // Exit with an error code.
        }

        String processDate = args[0]; // Get the process date from arguments.

        // Initialize the ETL class.
        FinancialTransactionETL etl = new FinancialTransactionETL();
        String projectId = etl.bigquery.getOptions().getProjectId();

        // Check if the Google Cloud Project ID is configured.
        if (projectId == null || projectId.trim().isEmpty()) {
            logger.log(Level.SEVERE, "Google Cloud Project ID not found. " +
                    "Please set the GOOGLE_CLOUD_PROJECT environment variable or configure application default credentials.");
            System.exit(1);
        }

        logger.log(Level.INFO, "Starting ETL application for Google Cloud Project: {0}", projectId);

        try {
            // Ensure necessary datasets exist before running the pipeline.
            etl.ensureBigQueryDatasets(projectId);

            // Execute the main ETL pipeline logic.
            etl.runEtlPipeline(processDate);
        } catch (Exception e) {
            // Catch any unexpected exceptions during the overall ETL process.
            logger.log(Level.SEVERE, "An unexpected error occurred during the ETL process for date {0}.", processDate);
            logger.log(Level.SEVERE, "Error details:", e);
            System.exit(1); // Exit with an error code.
        }
    }
}
```
