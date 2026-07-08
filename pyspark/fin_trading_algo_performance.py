import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.Window;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static org.apache.spark.sql.functions.*;

public class AlgorithmicTradingPerformance {

    /**
     * Simulates high-frequency trading performance evaluation.
     * It joins order logs (acks, fills, closures) with market data ticks to evaluate
     * slippage (PL), opportunity costs, and VWAP (Volume-Weighted Average Price) differences.
     */

    public String localToUtcTime(int hour, int minute, String dateStr, String tz) {
        ZoneId localZone = ZoneId.of(tz);
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr + " " + String.format("%02d", hour) + ":" + String.format("%02d", minute) + ":00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        ZonedDateTime zonedDateTime = localDateTime.atZone(localZone);
        return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public void executePipeline(SparkSession spark, String runDate) {

        // 1. Load Core Datasets
        Dataset<Row> allOrderEventsDf = spark.read().parquet("hdfs://trading_events_base/");
        Dataset<Row> parentOrdersDf = spark.read().parquet("hdfs://parent_orders/");

        // 2. Separate Event Stream into Fills
        // Anonymized protocol filtering conceptually representing status flags
        Column fillsCondition = (col("protocol_version").equalTo("V1")
                .and(col("status").isin("FILLED", "PARTIAL"))
                .and(col("event_type").equalTo("TRADE")))
                .or(col("protocol_version").geq("V2")
                        .and(col("event_type").equalTo("FILL")));

        Dataset<Row> fillsDf = allOrderEventsDf.filter(fillsCondition);

        // Get the closing fill price per order
        Dataset<Row> closeFillPriceDf = fillsDf
                .withColumn("rn", row_number().over(Window.partitionBy("order_id", "client_id").orderBy("event_timestamp")))
                .filter(col("rn").equalTo(1))
                .select(
                        col("order_id").as("close_order_id"),
                        col("client_id").as("close_client_id"),
                        col("last_exec_price").as("closing_price"),
                        col("last_exec_qty").as("closing_qty")
                )
                .drop("rn");

        // Aggregate fills per order
        Dataset<Row> fillsAggDf = fillsDf
                .groupBy("order_id", "client_id", "trade_date")
                .agg(
                        least(min("event_timestamp"), min("routing_timestamp")).as("FillStartTime"),
                        min("event_timestamp").as("FirstFillTime"),
                        sum("last_exec_qty").as("TotalSharesExecuted"),
                        count("last_exec_qty").as("NumberOfFills"),
                        (sum(col("last_exec_qty").multiply(col("last_exec_price"))).divide(sum("last_exec_qty"))).as("AverageExecutionPrice"),
                        sum(col("last_exec_qty").multiply(col("last_exec_price"))).as("TotalMarketValueExecuted")
                );

        // Prefix columns for joins
        for (String colName : fillsAggDf.columns()) {
            if (!colName.equals("order_id") && !colName.equals("client_id") && !colName.equals("trade_date")) {
                fillsAggDf = fillsAggDf.withColumnRenamed(colName, "fill_" + colName);
            }
        }

        // 3. Execution Acknowledgements
        Dataset<Row> acksDf = allOrderEventsDf.filter(col("status").isin("NEW", "REPLACED").and(col("event_type").equalTo("ACK")));
        Dataset<Row> acksAggDf = acksDf
                .groupBy("order_id", "client_id", "trade_date")
                .agg(least(min("event_timestamp"), min("routing_timestamp")).as("AckStartTime"));

        for (String colName : acksAggDf.columns()) {
            if (!colName.equals("order_id") && !colName.equals("client_id") && !colName.equals("trade_date")) {
                acksAggDf = acksAggDf.withColumnRenamed(colName, "ack_" + colName);
            }
        }

        // 4. Execution Terminations
        Dataset<Row> terminationsDf = allOrderEventsDf.filter(col("status").isin("CANCELED", "DONE_FOR_DAY", "EXPIRED", "REJECTED"));
        Dataset<Row> terminationsAggDf = terminationsDf
                .groupBy("order_id", "client_id", "trade_date")
                .agg(greatest(max("event_timestamp"), max("routing_timestamp")).as("ExecutionEndTime"));

        for (String colName : terminationsAggDf.columns()) {
            if (!colName.equals("order_id") && !colName.equals("client_id") && !colName.equals("trade_date")) {
                terminationsAggDf = terminationsAggDf.withColumnRenamed(colName, "term_" + colName);
            }
        }

        // 5. Bring it back to Parent Orders
        Dataset<Row> enrichedOrdersDf = parentOrdersDf.as("p")
                .join(fillsAggDf.as("f"),
                        col("p.order_id").equalTo(col("f.order_id")).and(col("p.client_id").equalTo(col("f.client_id"))),
                        "left")
                .join(acksAggDf.as("a"),
                        col("p.order_id").equalTo(col("a.order_id")).and(col("p.client_id").equalTo(col("a.client_id"))),
                        "left")
                .join(terminationsAggDf.as("t"),
                        col("p.order_id").equalTo(col("t.order_id")).and(col("p.client_id").equalTo(col("t.client_id"))),
                        "left")
                .join(closeFillPriceDf.as("c"),
                        col("p.order_id").equalTo(col("c.close_order_id")).and(col("p.client_id").equalTo(col("c.close_client_id"))),
                        "left")
                .drop(col("f.order_id"), col("f.client_id"), col("a.order_id"), col("a.client_id"), col("t.order_id"), col("t.client_id"), col("c.close_order_id"), col("c.close_client_id"));

        String utcTimeMarketOpen = localToUtcTime(9, 30, runDate, "America/New_York");
        String utcTimeMarketClose = localToUtcTime(16, 0, runDate, "America/New_York");

        // Establish effective operating window
        enrichedOrdersDf = enrichedOrdersDf
                .withColumn("EffectiveStartTime", least(greatest(col("ack_AckStartTime"), lit(utcTimeMarketOpen).cast("timestamp")), col("fill_FillStartTime")))
                .withColumn("EffectiveEndTime", least(col("term_ExecutionEndTime"), lit(utcTimeMarketClose).cast("timestamp")));

        // 6. Market Data Tick Metrics (complex time-based joins)
        Dataset<Row> quotesDf = spark.read().parquet("hdfs://level1_quotes/");

        quotesDf = quotesDf.repartition(col("ticker")).sortWithinPartitions("quote_timestamp");
        enrichedOrdersDf = enrichedOrdersDf.repartition(col("ticker")).sortWithinPartitions("EffectiveStartTime");

        quotesDf = quotesDf.withColumn("quote_pk", monotonically_increasing_id());
        enrichedOrdersDf = enrichedOrdersDf.withColumn("order_pk", monotonically_increasing_id());

        // Build Window buffers for Quote lookups
        enrichedOrdersDf = enrichedOrdersDf
                .withColumn("Start_lower", expr("EffectiveStartTime - interval '10 minutes'")) // Remediation Applied
                .withColumn("End_lower", expr("EffectiveEndTime - interval '10 minutes'"))   // Remediation Applied
                .withColumn("End_plus1", expr("EffectiveEndTime + interval '1 minute'"))     // Remediation Applied
                .withColumn("End_plus1_lower", expr("End_plus1 - interval '10 minutes'"))    // Remediation Applied
                .withColumn("End_plus5", expr("EffectiveEndTime + interval '5 minutes'"))     // Remediation Applied
                .withColumn("End_plus5_lower", expr("End_plus5 - interval '10 minutes'"));   // Remediation Applied

        // Define a function to reuse logic for looking up the nearest quote
        FindNearestQuoteFunction findNearestQuote = new FindNearestQuoteFunction();

        // Perform lookups
        Dataset<Row> openQuotes = findNearestQuote.apply(enrichedOrdersDf, quotesDf, "EffectiveStartTime", "Start_lower", "start");
        Dataset<Row> endQuotes = findNearestQuote.apply(enrichedOrdersDf, quotesDf, "EffectiveEndTime", "End_lower", "end");
        Dataset<Row> end1mQuotes = findNearestQuote.apply(enrichedOrdersDf, quotesDf, "End_plus1", "End_plus1_lower", "end_plus1");

        // 7. Core VWAP and Financial Performance calculations
        Dataset<Row> tradesDf = spark.read().parquet("hdfs://market_trades/");

        // VWAP during order existence
        Dataset<Row> vwapDf = enrichedOrdersDf
                .join(tradesDf,
                        enrichedOrdersDf.col("ticker").equalTo(tradesDf.col("ticker"))
                                .and(tradesDf.col("trade_timestamp").geq(enrichedOrdersDf.col("EffectiveStartTime")))
                                .and(tradesDf.col("trade_timestamp").leq(enrichedOrdersDf.col("EffectiveEndTime"))),
                        "inner")
                .groupBy("order_id", "client_id")
                .agg(
                        sum(tradesDf.col("trade_size")).as("market_interval_volume"),
                        (sum(tradesDf.col("trade_price").multiply(tradesDf.col("trade_size"))).divide(sum(tradesDf.col("trade_size")))).as("market_interval_vwap")
                );

        // Join everything back
        Dataset<Row> finalDf = enrichedOrdersDf
                .join(vwapDf, new String[]{"order_id", "client_id"}, "left")
                // Selectively bringing fields from quote buffers
                .join(openQuotes.select(col("order_pk"), col("start_best_bid"), col("start_best_ask"), col("start_quote_timestamp")), "order_pk", "left")
                .join(endQuotes.select(col("order_pk"), col("end_best_bid"), col("end_best_ask")), "order_pk", "left")
                .join(end1mQuotes.select(col("order_pk"), col("end_plus1_best_bid"), col("end_plus1_best_ask")), "order_pk", "left");

        // Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        finalDf = finalDf.withColumn("arrival_mid_price", (col("start_best_bid").plus(col("start_best_ask"))).divide(2));

        // Slippage from VWAP
        finalDf = finalDf.withColumn("slippage_from_vwap_bps",
                when(col("side").equalTo("BUY"), (col("fill_AverageExecutionPrice").minus(col("market_interval_vwap"))).divide(col("market_interval_vwap")).multiply(10000))
                        .when(col("side").equalTo("SELL"), (col("market_interval_vwap").minus(col("fill_AverageExecutionPrice"))).divide(col("market_interval_vwap")).multiply(10000))
        );

        // Slippage from Arrival Mid (Implementation Shortfall)
        finalDf = finalDf.withColumn("implementation_shortfall_pl",
                when(col("side").equalTo("BUY"), (col("arrival_mid_price").minus(col("fill_AverageExecutionPrice"))).multiply(col("fill_TotalSharesExecuted")))
                        .when(col("side").equalTo("SELL"), (col("fill_AverageExecutionPrice").minus(col("arrival_mid_price"))).multiply(col("fill_TotalSharesExecuted")))
        );

        // Momentum calculations post-trade
        finalDf = finalDf.withColumn("post_trade_1m_momentum",
                when(col("side").equalTo("BUY"), (((col("end_plus1_best_bid").plus(col("end_plus1_best_ask"))).divide(2)).minus((col("end_best_bid").plus(col("end_best_ask"))).divide(2))).multiply(col("fill_TotalSharesExecuted")))
                        .when(col("side").equalTo("SELL"), (((col("end_best_bid").plus(col("end_best_ask"))).divide(2)).minus((col("end_plus1_best_bid").plus(col("end_plus1_best_ask"))).divide(2))).multiply(col("fill_TotalSharesExecuted")))
        );

        finalDf = finalDf.withColumn("opportunity_cost_pl",
                when(col("side").equalTo("BUY"), (col("requested_shares").minus(col("fill_TotalSharesExecuted"))).multiply(col("fill_AverageExecutionPrice").minus(col("closing_price"))))
                        .when(col("side").equalTo("SELL"), (col("requested_shares").minus(col("fill_TotalSharesExecuted"))).multiply(col("closing_price").minus(col("fill_AverageExecutionPrice"))))
        );

        finalDf.write().mode("overwrite").parquet("hdfs://trading_analytics/run_date=" + runDate);
        System.out.println("Performance analysis complete.");
    }

    private static class FindNearestQuoteFunction {
        public Dataset<Row> apply(Dataset<Row> ordersDf, Dataset<Row> quotesDf, String targetTimeCol, String lowerBoundCol, String prefix) {
            Column joinCond = ordersDf.col("ticker").equalTo(quotesDf.col("ticker"))
                    .and(col("quote_timestamp").between(col(lowerBoundCol), col(targetTimeCol)));

            Dataset<Row> joined = ordersDf.join(quotesDf, joinCond, "inner")
                    .hint("merge")
                    .withColumn("time_diff", abs(col(targetTimeCol).minus(col("quote_timestamp"))));

            Dataset<Row> nearest = joined
                    .withColumn("rn", row_number().over(Window.partitionBy("order_pk").orderBy("time_diff")))
                    .filter(col("rn").equalTo(1))
                    .drop("rn", "time_diff", "Start_lower", "End_lower", "End_plus1", "End_plus1_lower", "End_plus5", "End_plus5_lower", quotesDf.col("ticker"));

            // Rename quote columns uniquely
            nearest = nearest.withColumnRenamed("quote_timestamp", prefix + "_quote_timestamp");
            nearest = nearest.withColumnRenamed("best_bid", prefix + "_best_bid");
            nearest = nearest.withColumnRenamed("best_ask", prefix + "_best_ask");
            return nearest;
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: AlgorithmicTradingPerformance <run_date>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("AlgorithmicTradingPerformance").getOrCreate();
        AlgorithmicTradingPerformance performanceAnalyzer = new AlgorithmicTradingPerformance();
        performanceAnalyzer.executePipeline(spark, args[0]);
        spark.stop();
    }
}
