import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.logging.Logger;
import java.util.logging.Level;

public class FinancialTransactionEtl {

    private static final Logger LOGGER = Logger.getLogger(FinancialTransactionEtl.class.getName());

    public static BigQuery createBigQueryClient() {
        LOGGER.info("Initializing BigQuery client...");
        // BigQuery client initializes typically through environment variables (e.g., GOOGLE_APPLICATION_CREDENTIALS)
        // or service account keys.
        return BigQueryOptions.newBuilder().build().getService();
    }

    private static void executeBigQuerySql(BigQuery bigquery, String sql) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        // Execute the query; it returns a Job that can be polled for completion.
        // For DDL/DML, the result table itself might be empty, but the job status indicates success/failure.
        TableResult result = bigquery.query(queryConfig);
        // Optionally, check result for errors or rows affected for DML
        if (result.getTotalRows() > 0 || result.getJob().getStatistics().getQuery().getNumDmlAffectedRows() > 0) {
            LOGGER.log(Level.INFO, "SQL execution complete. Rows affected/processed: {0}",
                    result.getJob().getStatistics().getQuery().getNumDmlAffectedRows());
        } else {
            LOGGER.info("SQL execution complete.");
        }
    }

    public static void runEtlPipeline(BigQuery bigquery, String processDate) throws InterruptedException {
        LOGGER.log(Level.INFO, "Starting execution for process_date: {0}", processDate);

        // BigQuery does not use Hive's SET hiveconf: process_date.
        // The process_date is directly substituted into the SQL queries using String.format.

        // 1. Create a temporary table in BigQuery to simulate Hive's TEMPORARY VIEW.
        // BigQuery temporary views typically only last for the duration of a single query job.
        // To be usable across multiple separate queries as intended here, a temporary table with
        // an expiration is a more direct functional equivalent.
        LOGGER.info("Creating temporary table for raw transactions...");
        String rawTrxDeltaTempTableName = "temp_raw_trx_delta_" + System.currentTimeMillis(); // Unique name for the temp table
        // This temporary table will be created in the `fin_landing` dataset and expire in 1 hour.
        String createRawTrxDeltaSql = String.format(
            "CREATE OR REPLACE TABLE fin_landing.%s OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS " +
            "SELECT " +
            "    trx_uuid, " +
            "    source_account_id, " +
            "    destination_account_id, " +
            "    transaction_type, " +
            "    amount_base_currency, " +
            "    currency_code, " +
            "    exchange_rate, " +
            "    transaction_timestamp, " +
            "    merchant_category_code, " +
            "    channel, " +
            "    status, " +
            "    error_code " +
            "FROM fin_landing.raw_transactions " +
            "WHERE DATE(transaction_timestamp) = '%s'", // BigQuery's DATE() function to extract date
            rawTrxDeltaTempTableName, processDate
        );
        LOGGER.log(Level.INFO, "Executing SQL for temporary raw transactions table:\n{0}", createRawTrxDeltaSql);
        executeBigQuerySql(bigquery, createRawTrxDeltaSql);


        // 2. Complex ETL to fact table using embedded SQL
        LOGGER.info("Inserting data into fin_core.fact_transactions...");
        // Hive's INSERT OVERWRITE TABLE ... PARTITION is translated to BigQuery as:
        // 1. DELETE data for the specific partition(s) for the given processDate.
        // 2. INSERT new data. This assumes 'fin_core.fact_transactions' is time-partitioned by 'trx_date'.
        String deleteFactTransactionsSql = String.format(
            "DELETE FROM fin_core.fact_transactions WHERE trx_date = '%s'",
            processDate
        );
        LOGGER.log(Level.INFO, "Executing DELETE for fin_core.fact_transactions:\n{0}", deleteFactTransactionsSql);
        executeBigQuerySql(bigquery, deleteFactTransactionsSql);

        String insertFactSql = String.format(
            "INSERT INTO fin_core.fact_transactions ( " +
            "    trx_uuid, source_account_id, destination_account_id, transaction_type, " +
            "    amount_base_currency, normalized_usd_amount, currency_code, " +
            "    transaction_timestamp, merchant_category_code, channel, customer_id, " +
            "    customer_segment, kyc_status, rolling_10_trx_amount, status, " +
            "    trx_date, region_id" +
            ") " +
            "SELECT " +
            "    r.trx_uuid, " +
            "    r.source_account_id, " +
            "    r.destination_account_id, " +
            "    r.transaction_type, " +
            "    r.amount_base_currency, " +
            "    CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC(38, 4)) AS normalized_usd_amount, " + // BigQuery's NUMERIC for exact decimal arithmetic
            "    r.currency_code, " +
            "    r.transaction_timestamp, " +
            "    r.merchant_category_code, " +
            "    r.channel, " +
            "    c.customer_id, " +
            "    c.customer_segment, " +
            "    c.kyc_status, " +
            "    SUM(r.amount_base_currency) OVER ( " +
            "        PARTITION BY r.source_account_id " +
            "        ORDER BY r.transaction_timestamp " +
            "        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW " +
            "    ) AS rolling_10_trx_amount, " +
            "    r.status, " +
            "    DATE(r.transaction_timestamp) AS trx_date, " + // BigQuery's DATE() function
            "    COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id " +
            "FROM fin_landing.%s r " + // Referencing the temporary table
            "LEFT JOIN fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id " +
            "LEFT JOIN fin_core.dim_customers c ON dim_a.customer_id = c.customer_id " +
            "WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE') " +
            "  AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'",
            rawTrxDeltaTempTableName
        );
        LOGGER.log(Level.INFO, "Executing INSERT for fin_core.fact_transactions:\n{0}", insertFactSql);
        executeBigQuerySql(bigquery, insertFactSql);


        // 3. Create Aggregated Datamart for Risk Analysis
        LOGGER.info("Executing aggregation for Risk Datamart...");
        // Similar conversion for INSERT OVERWRITE TABLE PARTITION to DELETE and then INSERT.
        // Assumes 'fin_mart.risk_daily_summary' is time-partitioned by 'summary_date'.
        String deleteRiskSummarySql = String.format(
            "DELETE FROM fin_mart.risk_daily_summary WHERE summary_date = '%s'",
            processDate
        );
        LOGGER.log(Level.INFO, "Executing DELETE for fin_mart.risk_daily_summary:\n{0}", deleteRiskSummarySql);
        executeBigQuerySql(bigquery, deleteRiskSummarySql);

        String insertRiskSql = String.format(
            "INSERT INTO fin_mart.risk_daily_summary ( " +
            "    customer_id, customer_segment, region_id, total_daily_transactions, " +
            "    total_daily_volume_usd, max_single_transaction_usd, high_risk_mcc_count, " +
            "    unique_destinations_count, daily_risk_flag, summary_date " +
            ") " +
            "SELECT " +
            "    customer_id, " +
            "    customer_segment, " +
            "    region_id, " +
            "    COUNT(trx_uuid) AS total_daily_transactions, " +
            "    SUM(normalized_usd_amount) AS total_daily_volume_usd, " +
            "    MAX(normalized_usd_amount) AS max_single_transaction_usd, " +
            "    COUNT(CASE WHEN merchant_category_code IN ('7995', '6012') THEN 1 END) AS high_risk_mcc_count, " +
            "    COUNT(DISTINCT destination_account_id) AS unique_destinations_count, " +
            "    CASE " +
            "        WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH' " +
            "        WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM' " +
            "        ELSE 'LOW' " +
            "    END AS daily_risk_flag, " +
            "    '%s' AS summary_date " + // Direct substitution
            "FROM fin_core.fact_transactions " +
            "WHERE trx_date = '%s' " + // Direct substitution
            "GROUP BY " +
            "    customer_id, " +
            "    customer_segment, " +
            "    region_id",
            processDate, processDate // Pass processDate twice for two substitutions
        );
        LOGGER.log(Level.INFO, "Executing INSERT for fin_mart.risk_daily_summary:\n{0}", insertRiskSql);
        executeBigQuerySql(bigquery, insertRiskSql);

        LOGGER.info("Successfully completed ETL pipeline.");
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: java FinancialTransactionEtl <YYYY-MM-DD>");
            System.exit(1);
        }

        String pDate = args[0];
        BigQuery bq = createBigQueryClient();

        // Initialize BigQuery datasets (equivalent to Hive databases/schemas).
        // Using `CREATE SCHEMA IF NOT EXISTS` is the DDL for datasets in BigQuery.
        // Added `default_table_expiration_days` as a common practice for datasets that may contain temporary data.
        LOGGER.info("Ensuring BigQuery datasets exist: fin_landing, fin_core, fin_mart");
        executeBigQuerySql(bq, "CREATE SCHEMA IF NOT EXISTS fin_landing OPTIONS(default_table_expiration_days=7)");
        executeBigQuerySql(bq, "CREATE SCHEMA IF NOT EXISTS fin_core OPTIONS(default_table_expiration_days=7)");
        executeBigQuerySql(bq, "CREATE SCHEMA IF NOT EXISTS fin_mart OPTIONS(default_table_expiration_days=7)");

        runEtlPipeline(bq, pDate);
        // BigQuery client resources are typically managed by Java's garbage collector. No explicit 'stop' is needed like SparkSession.
        LOGGER.info("ETL application finished.");
    }
}
