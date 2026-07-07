import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

class AlgorithmicTradingPerformance {

    /**
     * Simulates high-frequency trading performance evaluation.
     * It joins order logs (acks, fills, closures) with market data ticks to evaluate
     * slippage (PL), opportunity costs, and VWAP (Volume-Weighted Average Price) differences.
     */

    public String localToUtcTime(int hour, int minute, String dateStr, String tz) {
        // Equivalent to Python's default argument `tz='America/New_York'`
        if (tz == null || tz.isEmpty()) {
            tz = "America/New_York";
        }
        ZoneId localZoneId = ZoneId.of(tz);
        // Combine date from dateStr with hour and minute
        LocalDateTime datePart = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        LocalDateTime localDateTimeWithTime = datePart.withHour(hour).withMinute(minute).withSecond(0).withNano(0);

        ZonedDateTime zonedDateTime = localDateTimeWithTime.atZone(localZoneId);
        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public void executePipeline(BigQuery bigquery, String runDate) throws InterruptedException {
        // 1. Load Core Datasets
        // Python: all_order_events_df = spark.read.parquet("hdfs://trading_events_base/")
        // BigQuery: Reference an existing table/view.
        // `trading_events_base` is assumed to be an existing BigQuery table or view.
        String allOrderEventsDfView = "trading_events_base";

        // Python: parent_orders_df = spark.read.parquet("hdfs://parent_orders/")
        // `parent_orders` is assumed to be an existing BigQuery table or view.
        String parentOrdersDfView = "parent_orders";

        // 2. Separate Event Stream into Fills
        // Python: fills_df = all_order_events_df.filter(...)
        // Create a temporary view `fills_df` in BigQuery.
        String fillsDfView = "CREATE OR REPLACE TEMPORARY TABLE fills_df AS SELECT * FROM " + allOrderEventsDfView + " WHERE " +
                             "((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE') " +
                             "OR (protocol_version >= 'V2' AND event_type = 'FILL'))";
        executeBigQueryUpdate(bigquery, fillsDfView);

        // Python: close_fill_price_df = (fills_df.withColumn(...).filter(...).select(...).drop(...))
        // Create a temporary view `close_fill_price_df` with ranked fills.
        String closeFillPriceDfView = "CREATE OR REPLACE TEMPORARY TABLE close_fill_price_df AS " +
                                      "WITH RankedFills AS (" +
                                      "  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) as rn " +
                                      "  FROM fills_df" +
                                      ") " +
                                      "SELECT order_id AS close_order_id, client_id AS close_client_id, " +
                                      "       last_exec_price AS closing_price, last_exec_qty AS closing_qty " +
                                      "FROM RankedFills " +
                                      "WHERE rn = 1";
        executeBigQueryUpdate(bigquery, closeFillPriceDfView);

        // Python: fills_agg_df = (fills_df.groupBy(...).agg(...))
        // Create a temporary view `fills_agg_df` for aggregated fill data.
        String fillsAggDfView = "CREATE OR REPLACE TEMPORARY TABLE fills_agg_df AS " +
                                "SELECT order_id, client_id, trade_date, " +
                                "       LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS fill_FillStartTime, " +
                                "       MIN(event_timestamp) AS fill_FirstFillTime, " +
                                "       SUM(last_exec_qty) AS fill_TotalSharesExecuted, " +
                                "       COUNT(last_exec_qty) AS fill_NumberOfFills, " +
                                "       SAFE_DIVIDE(SUM(CAST(last_exec_qty AS BIGNUMERIC) * CAST(last_exec_price AS BIGNUMERIC)), SUM(CAST(last_exec_qty AS BIGNUMERIC))) AS fill_AverageExecutionPrice, " +
                                "       SUM(CAST(last_exec_qty AS BIGNUMERIC) * CAST(last_exec_price AS BIGNUMERIC)) AS fill_TotalMarketValueExecuted " +
                                "FROM fills_df " +
                                "GROUP BY order_id, client_id, trade_date";
        executeBigQueryUpdate(bigquery, fillsAggDfView);

        // Python: for col_name in fills_agg_df.columns: if col_name not in [...]: fills_agg_df = fills_agg_df.withColumnRenamed(col_name, "fill_" + col_name)
        // Column renaming is directly incorporated into the `fills_agg_df` SQL query using aliases.

        // 3. Execution Acknowledgements
        // Python: acks_df = all_order_events_df.filter(...)
        // Create a temporary view `acks_df`.
        String acksDfView = "CREATE OR REPLACE TEMPORARY TABLE acks_df AS SELECT * FROM " + allOrderEventsDfView + " WHERE " +
                            "status IN ('NEW', 'REPLACED') AND event_type = 'ACK'";
        executeBigQueryUpdate(bigquery, acksDfView);

        // Python: acks_agg_df = (acks_df.groupBy(...).agg(...))
        // Create a temporary view `acks_agg_df` for aggregated acknowledgment data.
        String acksAggDfView = "CREATE OR REPLACE TEMPORARY TABLE acks_agg_df AS " +
                               "SELECT order_id, client_id, trade_date, " +
                               "       LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime " +
                               "FROM acks_df " +
                               "GROUP BY order_id, client_id, trade_date";
        executeBigQueryUpdate(bigquery, acksAggDfView);

        // Python: for col_name in acks_agg_df.columns: if col_name not in [...]: acks_agg_df = acks_agg_df.withColumnRenamed(col_name, "ack_" + col_name)
        // Column renaming is directly incorporated into the `acks_agg_df` SQL query using aliases.

        // 4. Execution Terminations
        // Python: terminations_df = all_order_events_df.filter(...)
        // Create a temporary view `terminations_df`.
        String terminationsDfView = "CREATE OR REPLACE TEMPORARY TABLE terminations_df AS SELECT * FROM " + allOrderEventsDfView + " WHERE " +
                                    "status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')";
        executeBigQueryUpdate(bigquery, terminationsDfView);

        // Python: terminations_agg_df = (terminations_df.groupBy(...).agg(...))
        // Create a temporary view `terminations_agg_df` for aggregated termination data.
        String terminationsAggDfView = "CREATE OR REPLACE TEMPORARY TABLE terminations_agg_df AS " +
                                       "SELECT order_id, client_id, trade_date, " +
                                       "       GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime " +
                                       "FROM terminations_df " +
                                       "GROUP BY order_id, client_id, trade_date";
        executeBigQueryUpdate(bigquery, terminationsAggDfView);

        // Python: for col_name in terminations_agg_df.columns: if col_name not in [...]: terminations_agg_df = terminations_agg_df.withColumnRenamed(col_name, "term_" + col_name)
        // Column renaming is directly incorporated into the `terminations_agg_df` SQL query using aliases.

        // 5. Bring it back to Parent Orders
        // Python: enriched_orders_df = (parent_orders_df.alias("p").join(...).drop(...))
        // Create a temporary view `enriched_orders_df` by joining parent orders with aggregated event data.
        String enrichedOrdersDfView = "CREATE OR REPLACE TEMPORARY TABLE enriched_orders_df AS " +
                                      "SELECT p.*, " +
                                      "       f.fill_FillStartTime, f.fill_FirstFillTime, f.fill_TotalSharesExecuted, " +
                                      "       f.fill_NumberOfFills, f.fill_AverageExecutionPrice, f.fill_TotalMarketValueExecuted, " +
                                      "       a.ack_AckStartTime, t.term_ExecutionEndTime, c.closing_price, c.closing_qty " +
                                      "FROM " + parentOrdersDfView + " AS p " +
                                      "LEFT JOIN " + fillsAggDfView + " AS f ON p.order_id = f.order_id AND p.client_id = f.client_id " +
                                      "LEFT JOIN " + acksAggDfView + " AS a ON p.order_id = a.order_id AND p.client_id = a.client_id " +
                                      "LEFT JOIN " + terminationsAggDfView + " AS t ON p.order_id = t.order_id AND p.client_id = t.client_id " +
                                      "LEFT JOIN " + closeFillPriceDfView + " AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id";
        executeBigQueryUpdate(bigquery, enrichedOrdersDfView);

        // Python: utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        String utcTimeMarketOpen = localToUtcTime(9, 30, runDate, null);
        // Python: utc_time_market_close = self.local_to_utc_time(16, 00, run_date)
        String utcTimeMarketClose = localToUtcTime(16, 0, runDate, null);

        // Python: enriched_orders_df = (enriched_orders_df.withColumn(...))
        // Update `enriched_orders_df` with EffectiveStartTime and EffectiveEndTime.
        enrichedOrdersDfView = "CREATE OR REPLACE TEMPORARY TABLE enriched_orders_df AS " +
                               "SELECT *, " +
                               "       LEAST(GREATEST(ack_AckStartTime, TIMESTAMP('" + utcTimeMarketOpen + "')), fill_FillStartTime) AS EffectiveStartTime, " +
                               "       LEAST(term_ExecutionEndTime, TIMESTAMP('" + utcTimeMarketClose + "')) AS EffectiveEndTime " +
                               "FROM enriched_orders_df";
        executeBigQueryUpdate(bigquery, enrichedOrdersDfView);

        // 6. Market Data Tick Metrics (complex time-based joins)
        // Python: quotes_df = spark.read.parquet("hdfs://level1_quotes/")
        String quotesDfView = "level1_quotes";

        // Python: quotes_df = quotes_df.repartition("ticker").sortWithinPartitions("quote_timestamp")
        // Python: enriched_orders_df = enriched_orders_df.repartition("ticker").sortWithinPartitions("EffectiveStartTime")
        // Spark-specific partitioning and sorting hints. BigQuery handles optimization internally.
        // No direct BigQuery SQL statement for these as they don't alter the data logically.

        // Python: quotes_df = quotes_df.withColumn("quote_pk", F.monotonically_increasing_id())
        // Generate a row number as `quote_pk`.
        String quotesDfWithPkView = "CREATE OR REPLACE TEMPORARY TABLE quotes_df_with_pk AS " +
                                    "SELECT *, ROW_NUMBER() OVER () AS quote_pk FROM " + quotesDfView;
        executeBigQueryUpdate(bigquery, quotesDfWithPkView);

        // Python: enriched_orders_df = enriched_orders_df.withColumn("order_pk", F.monotonically_increasing_id())
        // Generate a row number as `order_pk`.
        String enrichedOrdersDfWithPkView = "CREATE OR REPLACE TEMPORARY TABLE enriched_orders_df AS " + // Reuse `enriched_orders_df` name
                                            "SELECT *, ROW_NUMBER() OVER () AS order_pk FROM enriched_orders_df";
        executeBigQueryUpdate(bigquery, enrichedOrdersDfWithPkView);

        // Python: enriched_orders_df = (enriched_orders_df.withColumn("Start_lower", F.expr(f"EffectiveStartTime - interval 10 minutes"))...)
        // Add time buffer columns to `enriched_orders_df`.
        enrichedOrdersDfView = "CREATE OR REPLACE TEMPORARY TABLE enriched_orders_df AS " +
                                      "SELECT *, " +
                                      "       DATETIME_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower, " +
                                      "       DATETIME_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower, " +
                                      "       DATETIME_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1, " +
                                      "       DATETIME_SUB(DATETIME_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower, " +
                                      "       DATETIME_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5, " +
                                      "       DATETIME_SUB(DATETIME_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower " +
                                      "FROM enriched_orders_df";
        executeBigQueryUpdate(bigquery, enrichedOrdersDfView);

        // Python: def find_nearest_quote(orders_df, quotes_df, target_time_col, lower_bound_col, prefix):
        // This is a helper method, `createNearestQuoteView`, defined below the `executePipeline` method.

        // Python: open_quotes = find_nearest_quote(enriched_orders_df, quotes_df, "EffectiveStartTime", "Start_lower", "start")
        String openQuotesView = createNearestQuoteView(bigquery, "enriched_orders_df", "quotes_df_with_pk", "EffectiveStartTime", "Start_lower", "start");

        // Python: end_quotes = find_nearest_quote(enriched_orders_df, quotes_df, "EffectiveEndTime", "End_lower", "end")
        String endQuotesView = createNearestQuoteView(bigquery, "enriched_orders_df", "quotes_df_with_pk", "EffectiveEndTime", "End_lower", "end");

        // Python: end_1m_quotes = find_nearest_quote(enriched_orders_df, quotes_df, "End_plus1", "End_plus1_lower", "end_plus1");
        String end1mQuotesView = createNearestQuoteView(bigquery, "enriched_orders_df", "quotes_df_with_pk", "End_plus1", "End_plus1_lower", "end_plus1");

        // 7. Core VWAP and Financial Performance calculations
        // Python: trades_df = spark.read.parquet("hdfs://market_trades/")
        String tradesDfView = "market_trades";

        // Python: vwap_df = (enriched_orders_df.join(trades_df, (...)).groupBy(...).agg(...))
        // Create a temporary view `vwap_df` for VWAP calculations.
        String vwapDfView = "CREATE OR REPLACE TEMPORARY TABLE vwap_df AS " +
                            "SELECT o.order_id, o.client_id, " +
                            "       SUM(t.trade_size) AS market_interval_volume, " +
                            "       SAFE_DIVIDE(SUM(CAST(t.trade_price AS BIGNUMERIC) * CAST(t.trade_size AS BIGNUMERIC)), SUM(CAST(t.trade_size AS BIGNUMERIC))) AS market_interval_vwap " +
                            "FROM enriched_orders_df AS o " +
                            "INNER JOIN " + tradesDfView + " AS t " +
                            "    ON o.ticker = t.ticker " +
                            "    AND t.trade_timestamp BETWEEN o.EffectiveStartTime AND o.EffectiveEndTime " +
                            "GROUP BY o.order_id, o.client_id";
        executeBigQueryUpdate(bigquery, vwapDfView);

        // Python: final_df = (enriched_orders_df.join(vwap_df, (...)).join(open_quotes.select(...))...)
        // Create `final_df` by joining all derived dataframes.
        String finalDfView = "CREATE OR REPLACE TEMPORARY TABLE final_df AS " +
                             "SELECT e.*, v.market_interval_volume, v.market_interval_vwap, " +
                             "       oq.start_best_bid, oq.start_best_ask, oq.start_quote_timestamp, " +
                             "       eq.end_best_bid, eq.end_best_ask, " +
                             "       e1q.end_plus1_best_bid, e1q.end_plus1_best_ask " +
                             "FROM enriched_orders_df AS e " +
                             "LEFT JOIN " + vwapDfView + " AS v ON e.order_id = v.order_id AND e.client_id = v.client_id " +
                             "LEFT JOIN " + openQuotesView + " AS oq ON e.order_pk = oq.order_pk " +
                             "LEFT JOIN " + endQuotesView + " AS eq ON e.order_pk = eq.order_pk " +
                             "LEFT JOIN " + end1mQuotesView + " AS e1q ON e.order_pk = e1q.order_pk";
        executeBigQueryUpdate(bigquery, finalDfView);

        // Python: final_df = final_df.withColumn("arrival_mid_price", (F.col("start_best_bid") + F.col("start_best_ask")) / 2)
        // Add `arrival_mid_price` to `final_df`.
        finalDfView = "CREATE OR REPLACE TEMPORARY TABLE final_df AS SELECT *, " +
                      "SAFE_DIVIDE((start_best_bid + start_best_ask), 2) AS arrival_mid_price " +
                      "FROM final_df";
        executeBigQueryUpdate(bigquery, finalDfView);

        // Python: final_df = final_df.withColumn("slippage_from_vwap_bps", F.when(...))
        // Add `slippage_from_vwap_bps` to `final_df`.
        finalDfView = "CREATE OR REPLACE TEMPORARY TABLE final_df AS SELECT *, " +
                      "CASE " +
                      "    WHEN side = 'BUY' THEN SAFE_MULTIPLY(SAFE_DIVIDE((fill_AverageExecutionPrice - market_interval_vwap), market_interval_vwap), 10000) " +
                      "    WHEN side = 'SELL' THEN SAFE_MULTIPLY(SAFE_DIVIDE((market_interval_vwap - fill_AverageExecutionPrice), market_interval_vwap), 10000) " +
                      "    ELSE NULL " +
                      "END AS slippage_from_vwap_bps " +
                      "FROM final_df";
        executeBigQueryUpdate(bigquery, finalDfView);

        // Python: final_df = final_df.withColumn("implementation_shortfall_pl", F.when(...))
        // Add `implementation_shortfall_pl` to `final_df`.
        finalDfView = "CREATE OR REPLACE TEMPORARY TABLE final_df AS SELECT *, " +
                                  "CASE " +
                                  "    WHEN side = 'BUY' THEN SAFE_MULTIPLY((arrival_mid_price - fill_AverageExecutionPrice), fill_TotalSharesExecuted) " +
                                  "    WHEN side = 'SELL' THEN SAFE_MULTIPLY((fill_AverageExecutionPrice - arrival_mid_price), fill_TotalSharesExecuted) " +
                                  "    ELSE NULL " +
                                  "END AS implementation_shortfall_pl " +
                                  "FROM final_df";
        executeBigQueryUpdate(bigquery, finalDfView);

        // Python: final_df = final_df.withColumn("post_trade_1m_momentum", F.when(...))
        // Add `post_trade_1m_momentum` to `final_df`.
        finalDfView = "CREATE OR REPLACE TEMPORARY TABLE final_df AS SELECT *, " +
                                      "CASE " +
                                      "    WHEN side = 'BUY' THEN SAFE_MULTIPLY(SAFE_DIVIDE((end_plus1_best_bid + end_plus1_best_ask), 2) - SAFE_DIVIDE((end_best_bid + end_best_ask), 2), fill_TotalSharesExecuted) " +
                                      "    WHEN side = 'SELL' THEN SAFE_MULTIPLY(SAFE_DIVIDE((end_best_bid + end_best_ask), 2) - SAFE_DIVIDE((end_plus1_best_bid + end_plus1_best_ask), 2), fill_TotalSharesExecuted) " +
                                      "    ELSE NULL " +
                                      "END AS post_trade_1m_momentum " +
                                      "FROM final_df";
        executeBigQueryUpdate(bigquery, finalDfView);

        // Python: final_df = final_df.withColumn("opportunity_cost_pl", F.when(...))
        // Add `opportunity_cost_pl` to `final_df`.
        finalDfView = "CREATE OR REPLACE TEMPORARY TABLE final_df AS SELECT *, " +
                                    "CASE " +
                                    "    WHEN side = 'BUY' THEN SAFE_MULTIPLY((requested_shares - fill_TotalSharesExecuted), (fill_AverageExecutionPrice - closing_price)) " +
                                    "    WHEN side = 'SELL' THEN SAFE_MULTIPLY((requested_shares - fill_TotalSharesExecuted), (closing_price - fill_AverageExecutionPrice)) " +
                                    "    ELSE NULL " +
                                    "END AS opportunity_cost_pl " +
                                    "FROM final_df";
        executeBigQueryUpdate(bigquery, finalDfView);

        // Python: final_df.write.parquet(f"hdfs://trading_analytics/run_date={run_date}", mode="overwrite")
        // BigQuery equivalent: Delete existing data for `runDate` and insert new results.
        // Assuming `trading_analytics` is the target table with a partition column `run_date_partition_col` (DATE type).
        String targetBigQueryTable = "trading_analytics";
        String deleteSql = "DELETE FROM " + targetBigQueryTable + " WHERE run_date_partition_col = PARSE_DATE('%Y-%m-%d', '" + runDate + "')";
        executeBigQueryUpdate(bigquery, deleteSql);

        // Note: The column list should match the `final_df` schema and target table schema.
        // The following list is representative, a real application would generate this dynamically or keep it updated.
        String finalInsertSql = "INSERT INTO " + targetBigQueryTable + " ( " +
                                "order_id, client_id, trade_date, ticker, side, requested_shares, " +
                                "fill_FillStartTime, fill_FirstFillTime, fill_TotalSharesExecuted, fill_NumberOfFills, fill_AverageExecutionPrice, fill_TotalMarketValueExecuted, " +
                                "ack_AckStartTime, term_ExecutionEndTime, closing_price, closing_qty, " +
                                "EffectiveStartTime, EffectiveEndTime, quote_pk, order_pk, Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower, " +
                                "market_interval_volume, market_interval_vwap, start_best_bid, start_best_ask, start_quote_timestamp, " +
                                "end_best_bid, end_best_ask, end_plus1_best_bid, end_plus1_best_ask, " +
                                "arrival_mid_price, slippage_from_vwap_bps, implementation_shortfall_pl, post_trade_1m_momentum, opportunity_cost_pl, " +
                                "run_date_partition_col" + // Column for date partitioning in BigQuery
                                " ) " +
                                "SELECT " +
                                "* REPLACE(PARSE_DATE('%Y-%m-%d', '" + runDate + "') AS run_date_partition_col) " + // Add run_date as a new column
                                "FROM final_df";
        executeBigQueryUpdate(bigquery, finalInsertSql);

        // Python: print("Performance analysis complete.")
        System.out.println("Performance analysis complete.");
    }

    // Helper method to execute BigQuery DDL/DML statements
    private void executeBigQueryUpdate(BigQuery bigquery, String query) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        bigquery.query(queryConfig); // Executes the query and waits for completion
    }

    // Python `find_nearest_quote` function translated to a private Java helper method.
    private String createNearestQuoteView(BigQuery bigquery, String ordersViewName, String quotesViewName,
                                          String targetTimeCol, String lowerBoundCol, String prefix) throws InterruptedException {
        String newViewName = prefix + "_quotes";
        // Spark `.hint("merge")` is a performance hint and has no direct SQL equivalent in BigQuery;
        // BigQuery's optimizer handles query planning.
        String sql = "CREATE OR REPLACE TEMPORARY TABLE " + newViewName + " AS " +
                     "WITH JoinedQuotes AS (" +
                     "    SELECT " +
                     "        o.order_pk, " +
                     "        q.quote_timestamp, q.best_bid, q.best_ask, " +
                     "        ABS(TIMESTAMP_DIFF(o." + targetTimeCol + ", q.quote_timestamp, MILLISECOND)) AS time_diff " +
                     "    FROM " + ordersViewName + " AS o " +
                     "    INNER JOIN " + quotesViewName + " AS q " +
                     "        ON o.ticker = q.ticker " +
                     "        AND q.quote_timestamp BETWEEN o." + lowerBoundCol + " AND o." + targetTimeCol +
                     "), " +
                     "RankedQuotes AS (" +
                     "    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff) as rn " +
                     "    FROM JoinedQuotes" +
                     ") " +
                     "SELECT order_pk, " +
                     "       quote_timestamp AS " + prefix + "_quote_timestamp, " +
                     "       best_bid AS " + prefix + "_best_bid, " +
                     "       best_ask AS " + prefix + "_best_ask " +
                     "FROM RankedQuotes " +
                     "WHERE rn = 1";
        executeBigQueryUpdate(bigquery, sql);
        return newViewName;
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            // Equivalent to Python's sys.exit(1)
            System.err.println("Usage: java AlgorithmicTradingPerformance <run_date>");
            System.exit(1);
        }

        // Initialize BigQuery client
        // Ensure GOOGLE_APPLICATION_CREDENTIALS environment variable is set for authentication
        BigQuery bigquery = com.google.cloud.bigquery.BigQueryOptions.getDefaultInstance().getService();

        AlgorithmicTradingPerformance p = new AlgorithmicTradingPerformance();
        p.executePipeline(bigquery, args[0]);
        // No explicit 'spark.stop()' equivalent for BigQuery client, it manages resources internally.
    }
}
