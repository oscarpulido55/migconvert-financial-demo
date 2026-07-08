```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

// Custom formatter to match the Python logging output format
class CustomLogFormatter extends Formatter {
    @Override
    public String format(LogRecord record) {
        return String.format("%s - %s - %s%n",
                             new java.util.Date(record.getMillis()),
                             record.getLevel().getName(),
                             formatMessage(record));
    }
}

public class FinancialTransactionETLBigQuery {

    // Configure logging
    private static final Logger logger = Logger.getLogger(FinancialTransactionETLBigQuery.class.getName());

    static {
        logger.setLevel(Level.INFO);
        // Remove default handlers to avoid duplicate console output
        for (Handler handler : logger.getHandlers()) {
            logger.removeHandler(handler);
        }
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setFormatter(new CustomLogFormatter());
        consoleHandler.setLevel(Level.INFO);
        logger.addHandler(consoleHandler);
    }

    /**
     * Initializes a BigQuery JDBC connection.
     * Assumes application default credentials (ADC) or gcloud authentication is set up.
     * For production, consider using a Service Account key file.
     */
    public static Connection createBigQueryConnection() throws SQLException {
        // IMPORTANT: Replace "YOUR_GCP_PROJECT_ID" with your actual Google Cloud Project ID.
        // This is necessary for BigQuery dataset paths and potentially for billing.
        // For OAuthType=2, ensure your environment is authenticated (e.g., via `gcloud auth application-default login`)
        String projectId = "YOUR_GCP_PROJECT_ID";
        String connectionUrl = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=%s;OAuthType=2;",
            projectId
        );
        // Load the BigQuery JDBC driver (implicitly done by DriverManager.getConnection in modern Java)
        // try {
        //     Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
        // } catch (ClassNotFoundException e) {
        //     throw new SQLException("BigQuery JDBC Driver not found", e);
        // }

        return DriverManager.getConnection(connectionUrl);
    }

    /**
     * Executes the ETL pipeline using embedded BigQuery SQL queries.
     * This demonstrates the capability of managing SQL directly within Java strings.
     */
    public static void runEtlPipeline(Connection connection, String processDate) throws SQLException {
        logger.info(String.format("Starting execution for process_date: %s", processDate));

        // BigQuery does not use Hiveconf session variables.
        // The processDate will be directly inserted into SQL queries via string formatting or PreparedStatement.
        // Using string formatting here for direct conversion, but PreparedStatements are recommended for security and performance.

        // Assuming a default project for datasets if not specified in connection string
        String projectId = "YOUR_GCP_PROJECT_ID"; // Use the same project ID as in createBigQueryConnection

        // 1. Create temporary table for the delta transactions from daily landing zone
        logger.info("Creating temporary table for raw transactions...");
        // BigQuery's CREATE TEMPORARY VIEW just stores the query; CREATE TEMPORARY TABLE materializes it.
        // For performance and functional equivalence to a 'delta' snapshot, a temporary table is often better.
        // 'OR REPLACE' works for CREATE TEMPORARY TABLE.
        String createTempTableSql = String.format(
            "CREATE OR REPLACE TEMPORARY TABLE raw_trx_delta AS " +
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
            "FROM %s.fin_landing.raw_transactions " + // Qualify table with project.dataset
            "WHERE CAST(transaction_timestamp AS DATE) = '%s'", // BigQuery: CAST as DATE or DATE() for to_date()
            projectId,
            processDate
        );
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTempTableSql);
        }

        // 2. Complex ETL to fact table using embedded SQL
        logger.info("Inserting data into fin_core.fact_transactions...");
        // Hive's INSERT OVERWRITE PARTITION is equivalent to DELETE then INSERT in BigQuery.
        // First, delete data for the target partition (trx_date = processDate).
        // A single process_date is expected from the temporary table.
        String deleteFactSql = String.format(
            "DELETE FROM %s.fin_core.fact_transactions " + // Qualify table
            "WHERE trx_date = '%s'",
            projectId,
            processDate
        );
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(deleteFactSql);
        }

        String insertFactSql = String.format(
            "INSERT INTO %s.fin_core.fact_transactions (" + // Qualify table
            "    trx_uuid, source_account_id, destination_account_id, transaction_type, amount_base_currency, " +
            "    normalized_usd_amount, currency_code, transaction_timestamp, merchant_category_code, channel, " +
            "    customer_id, customer_segment, kyc_status, rolling_10_trx_amount, status, trx_date, region_id" +
            ") " +
            "SELECT " +
            "    r.trx_uuid, " +
            "    r.source_account_id, " +
            "    r.destination_account_id, " +
            "    r.transaction_type, " +
            "    r.amount_base_currency, " +
            "    -- Calculate normalized amount for aggregations " +
            "    CAST(r.amount_base_currency * COALESCE(r.exchange_rate, 1.0) AS NUMERIC) AS normalized_usd_amount, " + // BigQuery NUMERIC for DECIMAL
            "    r.currency_code, " +
            "    r.transaction_timestamp, " +
            "    r.merchant_category_code, " +
            "    r.channel, " +
            "    c.customer_id, " +
            "    c.customer_segment, " +
            "    c.kyc_status, " +
            "    -- Rolling sum to flag consecutive large transactions " +
            "    SUM(r.amount_base_currency) OVER ( " +
            "        PARTITION BY r.source_account_id " +
            "        ORDER BY r.transaction_timestamp " +
            "        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW " +
            "    ) AS rolling_10_trx_amount, " +
            "    r.status, " +
            "    " +
            "    -- Partitioning Columns " +
            "    CAST(r.transaction_timestamp AS DATE) AS trx_date, " + // BigQuery: CAST as DATE
            "    COALESCE(dim_a.region_id, 'UNKNOWN') AS region_id " +
            "FROM raw_trx_delta r " +
            "LEFT JOIN %s.fin_core.dim_accounts dim_a ON r.source_account_id = dim_a.account_id " + // Qualify table
            "LEFT JOIN %s.fin_core.dim_customers c ON dim_a.customer_id = c.customer_id " + // Qualify table
            "WHERE r.status IN ('COMPLETED', 'SETTLED', 'PENDING_CLEARANCE') " +
            "  AND r.transaction_type != 'INTERNAL_TRANSFER_REVERSAL'",
            projectId,
            projectId,
            projectId
        );
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(insertFactSql);
        }

        // 3. Create Aggregated Datamart for Risk Analysis
        logger.info("Executing aggregation for Risk Datamart...");
        // Hive's INSERT OVERWRITE PARTITION is equivalent to DELETE then INSERT in BigQuery.
        // First, delete data for the target partition (summary_date = processDate).
        String deleteRiskSql = String.format(
            "DELETE FROM %s.fin_mart.risk_daily_summary " + // Qualify table
            "WHERE summary_date = '%s'",
            projectId,
            processDate
        );
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(deleteRiskSql);
        }

        String insertRiskSql = String.format(
            "INSERT INTO %s.fin_mart.risk_daily_summary (" + // Qualify table
            "    customer_id, customer_segment, region_id, total_daily_transactions, total_daily_volume_usd, " +
            "    max_single_transaction_usd, high_risk_mcc_count, unique_destinations_count, daily_risk_flag, summary_date" +
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
            "    -- Flag for Review " +
            "    CASE " +
            "        WHEN SUM(normalized_usd_amount) > 50000 AND customer_segment = 'RETAIL' THEN 'HIGH' " +
            "        WHEN COUNT(trx_uuid) > 100 THEN 'MEDIUM' " +
            "        ELSE 'LOW' " +
            "    END AS daily_risk_flag, " +
            "    '%s' AS summary_date " + // Directly use processDate
            "FROM %s.fin_core.fact_transactions " + // Qualify table
            "WHERE trx_date = '%s' " + // Directly use processDate
            "GROUP BY " +
            "    customer_id, " +
            "    customer_segment, " +
            "    region_id",
            projectId,
            processDate,
            projectId,
            processDate
        );
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(insertRiskSql);
        }

        logger.info("Successfully completed ETL pipeline.");
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java FinancialTransactionETLBigQuery <YYYY-MM-DD>");
            System.exit(1);
        }

        String processDate = args[0];
        Connection connection = null;

        try {
            connection = createBigQueryConnection();

            // Initialize BigQuery datasets (Hive databases) for safety
            // In BigQuery, this means creating datasets within your project.
            // Note: BigQuery JDBC driver generally expects datasets to exist.
            // Using CREATE SCHEMA IF NOT EXISTS, which maps to dataset creation.
            String projectId = "YOUR_GCP_PROJECT_ID"; // Use the same project ID as above
            try (Statement stmt = connection.createStatement()) {
                // BigQuery CREATE SCHEMA syntax includes the project_id.dataset_id
                stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s.fin_landing", projectId));
                stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s.fin_core", projectId));
                stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s.fin_mart", projectId));
            }

            runEtlPipeline(connection, processDate);

        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database error during ETL pipeline: " + e.getMessage(), e);
            System.exit(1);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "An unexpected error occurred: " + e.getMessage(), e);
            System.exit(1);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                    logger.info("BigQuery connection closed.");
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "Error closing BigQuery connection: " + e.getMessage(), e);
                }
            }
        }
    }
}
```
