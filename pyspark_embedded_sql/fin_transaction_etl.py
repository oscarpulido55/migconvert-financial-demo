import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.logging.Level;

// Converted from Python's logging setup and logger declaration
// This assumes these static members will be placed within a class context.
private static final Logger logger = Logger.getLogger("FinancialTransactionETL");
// Converted from Python's 'sp' (SparkSession) variable, now a BigQuery client instance.
private static BigQuery bigqueryClient;

// Helper method to encapsulate BigQuery SQL execution, equivalent to `spark.sql()`
private static void executeBigQuerySql(String sql, String queryDescription) throws InterruptedException {
    logger.log(Level.INFO, String.format("Executing BigQuery SQL for %s...", queryDescription));

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
            .setUseLegacySql(false) // Standard SQL is recommended for BigQuery
            .build();

    Job queryJob = bigqueryClient.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
        throw new RuntimeException("BigQuery Job no longer exists: " + jobId.getJob());
    } else if (queryJob.getStatus().getError() != null) {
        throw new RuntimeException(String.format("BigQuery SQL execution failed for %s. Error: %s",
                                                queryDescription, queryJob.getStatus().getError().toString()));
    }
    logger.log(Level.INFO, String.format("Successfully executed BigQuery SQL for %s.", queryDescription));
}

// Converted from Python's create_spark_session()
public static void setupBigQueryClient() {
    logger.log(Level.INFO, "Initializing BigQuery client.");
    // Initializes the BigQuery client using default credentials.
    bigqueryClient = BigQueryOptions.getDefaultInstance().getService();
    logger.log(Level.INFO, "BigQuery client initialized.");
    // Spark-specific configurations (e.g., dynamic partitioning) are not applicable to direct BigQuery client usage.
}

// Converted from Python's run_etl_pipeline(spark: SparkSession, process_date: str)
// The `spark` parameter is implicitly handled by using the static `bigqueryClient`.
public static void runEtlPipeline(String processDate) {
    logger.log(Level.INFO, String.format("Starting execution for process_date: %s", processDate));

    // Converted from: spark.sql(f"SET hiveconf:process_date='{process_date}'")
    // BigQuery does not use Hive-style configuration variables directly within SQL.
    // The `process_date` will be directly substituted into SQL queries via String.format().
    // For parameterized queries in production, consider QueryJobConfiguration.addNamedParameter().

    // 1. Create temporary view for the delta transactions from daily landing zone
    logger.log(Level.INFO, "Creating temporary view for raw transactions...");
    String rawTrxDeltaSql = String.format("""
        CREATE OR REPLACE TEMPORARY VIEW raw_trx_delta AS
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
        FROM fin_landing.raw_transactions
        WHERE DATE(transaction_timestamp) = DATE('%s')
        """, processDate); // Converted Hive's to_date() to BigQuery's DATE() function.

    try {
        executeBigQuerySql(rawTrxDeltaSql, "Create raw_trx_delta temporary view");
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Restore interrupt status
        logger.log(Level.SEVERE, "Error creating raw_trx_delta view", e);
        throw new RuntimeException("Failed to create raw_trx_delta view.", e);
    }

    // 2. Complex ETL to fact table using embedded SQL
    logger.log(Level.INFO, "Inserting data into fin_core.fact_transactions...");

    // Converted from: INSERT OVERWRITE TABLE fin_core.fact_transactions PARTITION (trx_date, region_id)
    // BigQuery's equivalent to Hive's INSERT OVERWRITE PARTITION typically involves an explicit DELETE
    // of existing data for the target partition(s), followed by an INSERT of new data.
    // This conversion assumes `trx_date` is the primary time-based partitioning column for `fact_transactions`,
    // and aims to overwrite all data within that date partition.
    String deleteFactSql = String.format("""
        DELETE FROM fin_core.fact_transactions WHERE trx_date = DATE('%s')
        """, processDate); // Deletes all data for the given process_date in the `trx_date` partition.

    String insertFactSql = String.format("""
        INSERT INTO fin_core.fact_transactions (
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
            -- Calculate normalized amount for aggregations
            CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS BIGNUMERIC) AS normalized_usd_amount, // Converted DECIMAL to BIGNUMERIC
            r.currency_code,
            r.transaction_timestamp,
            r.merchant_category_code,
            r.channel,
            c.customer_id,
            c.customer_segment,
            c.kyc_status,
            -- Rolling sum to flag consecutive large transactions (Standard SQL, compatible with BigQuery)
            SUM(r.amount_base_currency) OVER (
                PARTITION BY r.source_account_id
                ORDER BY r.transaction_timestamp
                ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
            ) AS rolling_10_trx_amount,
            r.status,

            -- Partitioning Columns
            DATE(r.transaction_timestamp) AS trx_date, // Converted to_date() to DATE()
            COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id
        FROM raw_trx_delta r
        LEFT JOIN fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id
        LEFT JOIN fin_core.dim_customers c ON dim_a.customer_id = c.customer_id
        WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE')
          AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'
        """); // The `PARTITION` clause is not part of BigQuery's `INSERT INTO` syntax; partitioning is defined in table DDL.

    try {
        executeBigQuerySql(deleteFactSql, "Delete existing fin_core.fact_transactions data for process_date");
        executeBigQuerySql(insertFactSql, "Insert into fin_core.fact_transactions");
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.log(Level.SEVERE, "Error inserting into fin_core.fact_transactions", e);
        throw new RuntimeException("Failed to insert into fin_core.fact_transactions.", e);
    }

    // 3. Create Aggregated Datamart for Risk Analysis
    logger.log(Level.INFO, "Executing aggregation for Risk Datamart...");

    // Converted from: INSERT OVERWRITE TABLE fin_mart.risk_daily_summary PARTITION (summary_date)
    // Similar to fact_transactions, overwrite in BigQuery is achieved via DELETE then INSERT.
    String deleteRiskSql = String.format("""
        DELETE FROM fin_mart.risk_daily_summary WHERE summary_date = DATE('%s')
        """, processDate); // Deletes all data for the given process_date in the `summary_date` partition.

    String insertRiskSql = String.format("""
        INSERT INTO fin_mart.risk_daily_summary (
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
            -- Flag for Review (Standard SQL, compatible with BigQuery)
            CASE
                WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH'
                WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS daily_risk_flag,
            DATE('%s') AS summary_date // Converted to using BigQuery's DATE() function
        FROM fin_core.fact_transactions
        WHERE trx_date = DATE('%s') // Converted to using BigQuery's DATE() function
        GROUP BY
            customer_id,
            customer_segment,
            region_id
        """, processDate, processDate);

    try {
        executeBigQuerySql(deleteRiskSql, "Delete existing fin_mart.risk_daily_summary data for process_date");
        executeBigQuerySql(insertRiskSql, "Insert into fin_mart.risk_daily_summary");
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.log(Level.SEVERE, "Error inserting into fin_mart.risk_daily_summary", e);
        throw new RuntimeException("Failed to insert into fin_mart.risk_daily_summary.", e);
    }

    logger.log(Level.INFO, "Successfully completed ETL pipeline.");
}

// Converted from Python's main execution block (if __name__ == "__main__":)
public static void main(String[] args) {
    if (args.length != 1) {
        System.out.println("Usage: FinancialTransactionETL <YYYY-MM-DD>"); // Assuming the class name is FinancialTransactionETL
        System.exit(1);
    }

    String p_date = args[0]; // Equivalent to sys.argv[1]

    // Initialize BigQuery client
    setupBigQueryClient(); // Converted from sp = create_spark_session()

    // Initialize BigQuery Datasets (equivalent to Hive databases)
    // Converted from: sp.sql("CREATE DATABASE IF NOT EXISTS ...")
    // BigQuery uses "Datasets" as logical containers for tables and views, analogous to Hive's "Databases".
    // `CREATE SCHEMA` in BigQuery SQL is the command to create a Dataset.
    try {
        executeBigQuerySql("CREATE SCHEMA IF NOT EXISTS fin_landing OPTIONS(description='Landing Zone for Financial Data')", "Create fin_landing dataset");
        executeBigQuerySql("CREATE SCHEMA IF NOT EXISTS fin_core OPTIONS(description='Core Financial Data Mart')", "Create fin_core dataset");
        executeBigQuerySql("CREATE SCHEMA IF NOT EXISTS fin_mart OPTIONS(description='Financial Risk Analysis Mart')", "Create fin_mart dataset");
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.log(Level.SEVERE, "Error initializing BigQuery datasets", e);
        System.exit(1);
    }

    try {
        runEtlPipeline(p_date);
    } catch (Exception e) {
        logger.log(Level.SEVERE, "ETL pipeline failed unexpectedly for process_date: " + p_date, e);
        System.exit(1);
    }
    // The BigQuery client generally does not require an explicit 'stop' or 'close' method call.
    logger.log(Level.INFO, "Application finished.");
}
