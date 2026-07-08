import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class AlgorithmicTradingPerformance {
    /*
     * Simulates high-frequency trading performance evaluation.
     * It joins order logs (acks, fills, closures) with market data ticks to evaluate
     * slippage (PL), opportunity costs, and VWAP (Volume-Weighted Average Price) differences.
     */

    /**
     * Converts a local time in a specified timezone to UTC.
     * @param hour Hour of the day.
     * @param minute Minute of the hour.
     * @param dateStr Date string in YYYY-MM-DD format.
     * @param tz Timezone, e.g., 'America/New_York'.
     * @return UTC time string in YYYY-MM-DD HH:MM:SS format.
     */
    public String local_to_utc_time(int hour, int minute, String dateStr, String tz) {
        ZoneId localZone = ZoneId.of(tz);
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime localDateTime = LocalDateTime.of(
            java.time.LocalDate.parse(dateStr, dateFormatter),
            java.time.LocalTime.of(hour, minute)
        );
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, localZone);
        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Executes the pipeline to simulate trading performance.
     * Converts PySpark DataFrame operations into BigQuery SQL queries executed via Java client.
     * @param bigquery The BigQuery client instance.
     * @param runDate The date for which to run the analysis (YYYY-MM-DD).
     * @throws InterruptedException If the BigQuery job is interrupted.
     */
    public void executePipeline(BigQuery bigquery, String runDate) throws InterruptedException {
        // Define BigQuery project and dataset for consistency.
        // --- Configuration change: Replace with your actual project and dataset IDs ---
        String projectId = "your_project_id";
        String datasetId = "your_dataset_id";

        // Table name conversions from HDFS paths to BigQuery fully qualified table names.
        String tradingEventsTable = String.format("`%s.%s.trading_events_base`", projectId, datasetId);
        String parentOrdersTable = String.format("`%s.%s.parent_orders`", projectId, datasetId);
        String level1QuotesTable = String.format("`%s.%s.level1_quotes`", projectId, datasetId);
        String marketTradesTable = String.format("`%s.%s.market_trades`", projectId, datasetId);
        String outputTable = String.format("`%s.%s.trading_analytics`", projectId, datasetId);

        // Building the main SQL query using Common Table Expressions (CTEs)
        StringBuilder sqlBuilder = new StringBuilder();

        // 2. Separate Event Stream into Fills (fills_df and close_fill_price_df)
        // This CTE handles initial filtering and preparation for fill-related aggregations.
        sqlBuilder.append("WITH Fills AS (\n")
                  .append("  SELECT\n")
                  .append("    order_id, client_id, trade_date, event_timestamp, routing_timestamp, last_exec_price, last_exec_qty,\n")
                  // PySpark F.row_number().over(Window.partitionBy(...).orderBy(...)) becomes BigQuery ROW_NUMBER() OVER (...)
                  .append("    ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) AS rn\n")
                  .append("  FROM ").append(tradingEventsTable).append("\n")
                  .append("  WHERE\n")
                  .append("    ((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')\n")
                  .append("    OR (protocol_version >= 'V2' AND event_type = 'FILL'))\n")
                  .append("),\n");

        // CTE for closing fill price, derived from the Fills CTE.
        sqlBuilder.append("CloseFillPrice AS (\n")
                  .append("  SELECT\n")
                  .append("    order_id AS close_order_id, client_id AS close_client_id,\n")
                  .append("    last_exec_price AS closing_price, last_exec_qty AS closing_qty\n")
                  .append("  FROM Fills\n")
                  .append("  WHERE rn = 1\n")
                  .append("),\n");

        // CTE for aggregated fills, mimicking fills_agg_df in PySpark.
        // PySpark `F.least(F.min("event_timestamp"), F.min("routing_timestamp"))`
        // becomes BigQuery `LEAST(MIN(event_timestamp), MIN(routing_timestamp))`
        // `SAFE_DIVIDE` is used for robustness against division by zero in BigQuery.
        sqlBuilder.append("FillsAgg AS (\n")
                  .append("  SELECT\n")
                  .append("    order_id, client_id, trade_date,\n")
                  .append("    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS fill_FillStartTime,\n")
                  .append("    MIN(event_timestamp) AS fill_FirstFillTime,\n")
                  .append("    SUM(last_exec_qty) AS fill_TotalSharesExecuted,\n")
                  .append("    COUNT(last_exec_qty) AS fill_NumberOfFills,\n")
                  .append("    SAFE_DIVIDE(SUM(last_exec_qty * last_exec_price), SUM(last_exec_qty)) AS fill_AverageExecutionPrice,\n")
                  .append("    SUM(last_exec_qty * last_exec_price) AS fill_TotalMarketValueExecuted\n")
                  .append("  FROM Fills\n")
                  .append("  GROUP BY order_id, client_id, trade_date\n")
                  .append("),\n");

        // 3. Execution Acknowledgements (acks_agg_df equivalent)
        sqlBuilder.append("AcksAgg AS (\n")
                  .append("  SELECT\n")
                  .append("    order_id, client_id, trade_date,\n")
                  .append("    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime\n")
                  .append("  FROM ").append(tradingEventsTable).append("\n")
                  .append("  WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'\n")
                  .append("  GROUP BY order_id, client_id, trade_date\n")
                  .append("),\n");

        // 4. Execution Terminations (terminations_agg_df equivalent)
        sqlBuilder.append("TerminationsAgg AS (\n")
                  .append("  SELECT\n")
                  .append("    order_id, client_id, trade_date,\n")
                  .append("    GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime\n")
                  .append("  FROM ").append(tradingEventsTable).append("\n")
                  .append("  WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')\n")
                  .append("  GROUP BY order_id, client_id, trade_date\n")
                  .append("),\n");

        // 5. Join all event types with Parent Orders to create EnrichedOrdersBase.
        // This includes calculating EffectiveStartTime and EffectiveEndTime.
        // PySpark `F.monotonically_increasing_id()` is replaced by BigQuery `GENERATE_UUID()`.
        // This generates a unique identifier for each row to serve as a stable `order_pk` for subsequent joins,
        // similar to its role in Spark for windowing operations.
        String utcTimeMarketOpen = local_to_utc_time(9, 30, runDate, "America/New_York");
        String utcTimeMarketClose = local_to_utc_time(16, 0, runDate, "America/New_York");

        sqlBuilder.append("EnrichedOrdersBase AS (\n")
                  .append("  SELECT\n")
                  .append("    p.* EXCEPT(order_id, client_id), -- Select all parent_orders columns, exclude common join keys to avoid ambiguity\n")
                  .append("    p.order_id, p.client_id,\n")
                  .append("    f.fill_FillStartTime,\n")
                  .append("    f.fill_FirstFillTime,\n")
                  .append("    f.fill_TotalSharesExecuted,\n")
                  .append("    f.fill_NumberOfFills,\n")
                  .append("    f.fill_AverageExecutionPrice,\n")
                  .append("    f.fill_TotalMarketValueExecuted,\n")
                  .append("    a.ack_AckStartTime,\n")
                  .append("    t.term_ExecutionEndTime,\n")
                  .append("    c.closing_price,\n")
                  .append("    c.closing_qty,\n")
                  .append("    GENERATE_UUID() AS order_pk, -- Functionally equivalent to PySpark's monotonically_increasing_id for this use case.\n")
                  .append("    LEAST(GREATEST(a.ack_AckStartTime, TIMESTAMP('").append(utcTimeMarketOpen).append("')), f.fill_FillStartTime) AS EffectiveStartTime,\n")
                  .append("    LEAST(t.term_ExecutionEndTime, TIMESTAMP('").append(utcTimeMarketClose).append("')) AS EffectiveEndTime\n")
                  .append("  FROM ").append(parentOrdersTable).append(" AS p\n")
                  .append("  LEFT JOIN FillsAgg AS f ON p.order_id = f.order_id AND p.client_id = f.client_id\n")
                  .append("  LEFT JOIN AcksAgg AS a ON p.order_id = a.order_id AND p.client_id = a.client_id\n")
                  .append("  LEFT JOIN TerminationsAgg AS t ON p.order_id = t.order_id AND p.client_id = t.client_id\n")
                  .append("  LEFT JOIN CloseFillPrice AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id\n")
                  .append("),\n");

        // 6. Market Data Tick Metrics - Build Window buffers for Quote lookups.
        // PySpark F.expr(f"EffectiveStartTime - interval 10 minutes") translates to BigQuery TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE).
        // BigQuery's interval syntax requires the unit to be singular (MINUTE, SECOND, etc.).
        sqlBuilder.append("EnrichedOrdersWithQuoteBuffers AS (\n")
                  .append("  SELECT\n")
                  .append("    *,\n")
                  .append("    TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,\n")
                  .append("    TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,\n")
                  .append("    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,\n")
                  .append("    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,\n")
                  .append("    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,\n")
                  .append("    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower\n")
                  .append("  FROM EnrichedOrdersBase\n")
                  .append("),\n");

        // CTE for Start Quotes: This section translates the `find_nearest_quote` Python logic to BigQuery SQL.
        // It uses `QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ABS(TIMESTAMP_DIFF(...))) = 1`
        // to find the nearest quote within a specified time window efficiently.
        sqlBuilder.append("StartQuotes AS (\n")
                  .append("  SELECT\n")
                  .append("    o.order_pk, -- Key for joining back to orders\n")
                  .append("    q.quote_timestamp AS start_quote_timestamp,\n")
                  .append("    q.best_bid AS start_best_bid,\n")
                  .append("    q.best_ask AS start_best_ask\n")
                  .append("  FROM EnrichedOrdersWithQuoteBuffers AS o\n")
                  .append("  INNER JOIN ").append(level1QuotesTable).append(" AS q ON o.ticker = q.ticker\n")
                  .append("  WHERE\n")
                  .append("    q.quote_timestamp BETWEEN o.Start_lower AND o.EffectiveStartTime\n")
                  .append("  QUALIFY ROW_NUMBER() OVER (\n")
                  .append("    PARTITION BY o.order_pk\n")
                  .append("    ORDER BY ABS(TIMESTAMP_DIFF(o.EffectiveStartTime, q.quote_timestamp, MILLISECOND))\n")
                  .append("  ) = 1\n")
                  .append("),\n");

        // CTE for End Quotes
        sqlBuilder.append("EndQuotes AS (\n")
                  .append("  SELECT\n")
                  .append("    o.order_pk,\n")
                  .append("    q.best_bid AS end_best_bid,\n")
                  .append("    q.best_ask AS end_best_ask\n")
                  .append("  FROM EnrichedOrdersWithQuoteBuffers AS o\n")
                  .append("  INNER JOIN ").append(level1QuotesTable).append(" AS q ON o.ticker = q.ticker\n")
                  .append("  WHERE\n")
                  .append("    q.quote_timestamp BETWEEN o.End_lower AND o.EffectiveEndTime\n")
                  .append("  QUALIFY ROW_NUMBER() OVER (\n")
                  .append("    PARTITION BY o.order_pk\n")
                  .append("    ORDER BY ABS(TIMESTAMP_DIFF(o.EffectiveEndTime, q.quote_timestamp, MILLISECOND))\n")
                  .append("  ) = 1\n")
                  .append("),\n");

        // CTE for End 1M Quotes
        sqlBuilder.append("End1MQuotes AS (\n")
                  .append("  SELECT\n")
                  .append("    o.order_pk,\n")
                  .append("    q.best_bid AS end_plus1_best_bid,\n")
                  .append("    q.best_ask AS end_plus1_best_ask\n")
                  .append("  FROM EnrichedOrdersWithQuoteBuffers AS o\n")
                  .append("  INNER JOIN ").append(level1QuotesTable).append(" AS q ON o.ticker = q.ticker\n")
                  .append("  WHERE\n")
                  .append("    q.quote_timestamp BETWEEN o.End_plus1_lower AND o.End_plus1\n")
                  .append("  QUALIFY ROW_NUMBER() OVER (\n")
                  .append("    PARTITION BY o.order_pk\n")
                  .append("    ORDER BY ABS(TIMESTAMP_DIFF(o.End_plus1, q.quote_timestamp, MILLISECOND))\n")
                  .append("  ) = 1\n")
                  .append("),\n");

        // 7. Core VWAP and Financial Performance calculations
        sqlBuilder.append("VWAPCalculations AS (\n")
                  .append("  SELECT\n")
                  .append("    o.order_id,\n")
                  .append("    o.client_id,\n")
                  .append("    SUM(t.trade_size) AS market_interval_volume,\n")
                  .append("    SAFE_DIVIDE(SUM(t.trade_price * t.trade_size), SUM(t.trade_size)) AS market_interval_vwap\n")
                  .append("  FROM EnrichedOrdersWithQuoteBuffers AS o\n")
                  .append("  INNER JOIN ").append(marketTradesTable).append(" AS t\n")
                  .append("    ON o.ticker = t.ticker\n")
                  .append("    AND t.trade_timestamp BETWEEN o.EffectiveStartTime AND o.EffectiveEndTime\n")
                  .append("  GROUP BY o.order_id, o.client_id\n")
                  .append(")\n"); // No comma after the last CTE before the final SELECT

        // Final SELECT statement joining everything and calculating metrics.
        // PySpark `drop` and `select` with exclusions translate to BigQuery `EXCEPT` clause.
        sqlBuilder.append("SELECT\n")
                  .append("  eo.* EXCEPT (order_pk, Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- Drop temporary columns\n")
                  .append("  vwap.market_interval_volume,\n")
                  .append("  vwap.market_interval_vwap,\n")
                  .append("  sq.start_best_bid,\n")
                  .append("  sq.start_best_ask,\n")
                  .append("  sq.start_quote_timestamp,\n")
                  .append("  eq.end_best_bid,\n")
                  .append("  eq.end_best_ask,\n")
                  .append("  e1mq.end_plus1_best_bid,\n")
                  .append("  e1mq.end_plus1_best_ask,\n")
                  .append("  (sq.start_best_bid + sq.start_best_ask) / 2 AS arrival_mid_price,\n")
                  // Slippage from VWAP: PySpark `F.when().when()` becomes BigQuery `CASE WHEN ... THEN ... END`
                  .append("  CASE\n")
                  .append("    WHEN eo.side = 'BUY' THEN SAFE_DIVIDE((eo.fill_AverageExecutionPrice - vwap.market_interval_vwap), vwap.market_interval_vwap) * 10000\n")
                  .append("    WHEN eo.side = 'SELL' THEN SAFE_DIVIDE((vwap.market_interval_vwap - eo.fill_AverageExecutionPrice), vwap.market_interval_vwap) * 10000\n")
                  .append("    ELSE NULL\n")
                  .append("  END AS slippage_from_vwap_bps,\n")
                  // Slippage from Arrival Mid (Implementation Shortfall)
                  .append("  CASE\n")
                  .append("    WHEN eo.side = 'BUY' THEN ( (sq.start_best_bid + sq.start_best_ask) / 2 - eo.fill_AverageExecutionPrice ) * eo.fill_TotalSharesExecuted\n")
                  .append("    WHEN eo.side = 'SELL' THEN ( eo.fill_AverageExecutionPrice - (sq.start_best_bid + sq.start_best_ask) / 2 ) * eo.fill_TotalSharesExecuted\n")
                  .append("    ELSE NULL\n")
                  .append("  END AS implementation_shortfall_pl,\n")
                  // Momentum calculations post-trade
                  .append("  CASE\n")
                  .append("    WHEN eo.side = 'BUY' THEN ( (e1mq.end_plus1_best_bid + e1mq.end_plus1_best_ask) / 2 - (eq.end_best_bid + eq.end_best_ask) / 2 ) * eo.fill_TotalSharesExecuted\n")
                  .append("    WHEN eo.side = 'SELL' THEN ( (eq.end_best_bid + eq.end_best_ask) / 2 - (e1mq.end_plus1_best_bid + e1mq.end_plus1_best_ask) / 2 ) * eo.fill_TotalSharesExecuted\n")
                  .append("    ELSE NULL\n")
                  .append("  END AS post_trade_1m_momentum,\n")
                  // Opportunity Cost
                  .append("  CASE\n")
                  .append("    WHEN eo.side = 'BUY' THEN (eo.requested_shares - eo.fill_TotalSharesExecuted) * (eo.fill_AverageExecutionPrice - eo.closing_price)\n")
                  .append("    WHEN eo.side = 'SELL' THEN (eo.requested_shares - eo.fill_TotalSharesExecuted) * (eo.closing_price - eo.fill_AverageExecutionPrice)\n")
                  .append("    ELSE NULL\n")
                  .append("  END AS opportunity_cost_pl\n")
                  .append("FROM EnrichedOrdersWithQuoteBuffers AS eo\n")
                  .append("LEFT JOIN VWAPCalculations AS vwap ON eo.order_id = vwap.order_id AND eo.client_id = vwap.client_id\n")
                  .append("LEFT JOIN StartQuotes AS sq ON eo.order_pk = sq.order_pk\n")
                  .append("LEFT JOIN EndQuotes AS eq ON eo.order_pk = eq.order_pk\n")
                  .append("LEFT JOIN End1MQuotes AS e1mq ON eo.order_pk = e1mq.order_pk;\n");

        String finalSqlQuery = sqlBuilder.toString();
        // --- Output target: PySpark final_df.write.parquet(...) becomes BigQuery CREATE OR REPLACE TABLE AS SELECT ---
        // This query overwrites the output table each run. Example partitioning and labeling are added.
        String finalOutputSql = "CREATE OR REPLACE TABLE " + outputTable + "\n"
                               + "PARTITION BY DATE(EffectiveStartTime)\n" // Performance consideration: Partitioning by date
                               + "OPTIONS(labels=[('run_date', '" + runDate.replace("-", "_") + "')]) AS\n" // Metadata: Add run_date as a label
                               + finalSqlQuery;

        // --- BigQuery execution block: Submits the constructed SQL query to BigQuery ---
        JobId jobId = JobId.of(UUID.randomUUID().toString()); // Generate a unique job ID
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(finalOutputSql)
                                                // BigQuery uses Standard SQL by default; .setUseLegacySql(false) is often redundant.
                                                .build();

        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        try {
            // Wait for the query to complete
            queryJob = queryJob.waitFor();

            // Check for errors
            if (queryJob.getStatus().getError() != null) {
                System.err.println("BigQuery query failed: " + queryJob.getStatus().getError().toString());
                throw new RuntimeException("BigQuery query failed: " + queryJob.getStatus().getError().getMessage());
            } else {
                System.out.println("Performance analysis complete. Results written to " + outputTable);
            }
        } catch (InterruptedException e) {
            System.err.println("BigQuery query interrupted: " + e.getMessage());
            // Re-throw the interruption for proper handling higher up the call stack
            throw e;
        }
        // --- End BigQuery execution block ---
    }

    /**
     * Main method to run the algorithmic trading performance analysis.
     * Takes run_date as a command-line argument.
     * @param args Command line arguments, expects one argument: run_date (YYYY-MM-DD).
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        // --- PySpark `sys.argv` equivalent in Java ---
        if (args.length < 1) {
            System.err.println("Usage: java AlgorithmicTradingPerformance <run_date (YYYY-MM-DD)>");
            System.exit(1);
        }

        String runDate = args[0];

        // --- Configuration change: Initialize BigQuery client ---
        // This typically picks up credentials from GOOGLE_APPLICATION_CREDENTIALS environment variable
        // or a default service account in a Google Cloud environment.
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        AlgorithmicTradingPerformance p = new AlgorithmicTradingPerformance();
        p.executePipeline(bigquery, runDate);
        // Note: There is no direct `spark.stop()` equivalent as BigQuery client is managed differently.
        // The BigQuery client does not hold persistent connections that need explicit closing for simple queries.
    }
}
