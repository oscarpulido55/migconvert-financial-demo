class AlgorithmicTradingPerformance {
    /*
     * Simulates high-frequency trading performance evaluation.
     * It joins order logs (acks, fills, closures) with market data ticks to evaluate
     * slippage (PL), opportunity costs, and VWAP (Volume-Weighted Average Price) differences.
     */

    private String localToUtcTime(int hour, int minute, String dateStr, String tz) {
        // Equivalent to Python's pytz and datetime for timezone handling and formatting.
        // Using Java 8 Date and Time API.
        ZoneId localZoneId = ZoneId.of(tz);
        LocalDate dateObj = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
        LocalDateTime localDateTime = LocalDateTime.of(dateObj.getYear(), dateObj.getMonth(), dateObj.getDayOfMonth(), hour, minute);
        ZonedDateTime zonedLocalTime = ZonedDateTime.of(localDateTime, localZoneId);
        ZonedDateTime zonedUtcTime = zonedLocalTime.withZoneSameInstant(ZoneId.of("UTC"));
        return zonedUtcTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public void executePipeline(SparkSession spark, String runDate) {

        // 1. Load Core Datasets
        Dataset<Row> allOrderEventsDf = spark.read().parquet("gs://trading_events_base/");
        Dataset<Row> parentOrdersDf = spark.read().parquet("gs://parent_orders/");

        // 2. Separate Event Stream into Fills
        // Anonymized protocol filtering conceptually representing status flags
        Dataset<Row> fillsDf = allOrderEventsDf.filter(
                functions.col("protocol_version").equalTo("V1").and(functions.col("status").isin("FILLED", "PARTIAL")).and(functions.col("event_type").equalTo("TRADE"))
                .or(functions.col("protocol_version").geq("V2").and(functions.col("event_type").equalTo("FILL")))
        );

        // Get the closing fill price per order
        WindowSpec closingFillWindow = Window.partitionBy("order_id", "client_id").orderBy("event_timestamp");
        Dataset<Row> closeFillPriceDf = fillsDf
            .withColumn("rn", functions.row_number().over(closingFillWindow))
            .filter(functions.col("rn").equalTo(1))
            .select(
                functions.col("order_id").alias("close_order_id"),
                functions.col("client_id").alias("close_client_id"),
                functions.col("last_exec_price").alias("closing_price"),
                functions.col("last_exec_qty").alias("closing_qty")
            )
            .drop("rn");

        // Aggregate fills per order
        Dataset<Row> fillsAggDf = fillsDf
            .groupBy("order_id", "client_id", "trade_date")
            .agg(
                functions.least(functions.min("event_timestamp"), functions.min("routing_timestamp")).alias("FillStartTime"),
                functions.min("event_timestamp").alias("FirstFillTime"),
                functions.sum("last_exec_qty").alias("TotalSharesExecuted"),
                functions.count("last_exec_qty").alias("NumberOfFills"),
                functions.callUDF("`divide`", functions.sum(functions.col("last_exec_qty").multiply(functions.col("last_exec_price"))), functions.sum("last_exec_qty")).alias("AverageExecutionPrice"),
                functions.sum(functions.col("last_exec_qty").multiply(functions.col("last_exec_price"))).alias("TotalMarketValueExecuted")
            );

        // Prefix columns for joins
        String[] fillsAggColumns = fillsAggDf.columns();
        for (String colName : fillsAggColumns) {
            if (!Arrays.asList("order_id", "client_id", "trade_date").contains(colName)) {
                fillsAggDf = fillsAggDf.withColumnRenamed(colName, "fill_" + colName);
            }
        }

        // 3. Execution Acknowledgements
        Dataset<Row> acksDf = allOrderEventsDf.filter(functions.col("status").isin("NEW", "REPLACED").and(functions.col("event_type").equalTo("ACK")));
        Dataset<Row> acksAggDf = acksDf
            .groupBy("order_id", "client_id", "trade_date")
            .agg(functions.least(functions.min("event_timestamp"), functions.min("routing_timestamp")).alias("AckStartTime"));

        String[] acksAggColumns = acksAggDf.columns();
        for (String colName : acksAggColumns) {
            if (!Arrays.asList("order_id", "client_id", "trade_date").contains(colName)) {
                acksAggDf = acksAggDf.withColumnRenamed(colName, "ack_" + colName);
            }
        }

        // 4. Execution Terminations
        Dataset<Row> terminationsDf = allOrderEventsDf.filter(functions.col("status").isin("CANCELED", "DONE_FOR_DAY", "EXPIRED", "REJECTED"));
        Dataset<Row> terminationsAggDf = terminationsDf
            .groupBy("order_id", "client_id", "trade_date")
            .agg(functions.greatest(functions.max("event_timestamp"), functions.max("routing_timestamp")).alias("ExecutionEndTime"));

        String[] terminationsAggColumns = terminationsAggDf.columns();
        for (String colName : terminationsAggColumns) {
            if (!Arrays.asList("order_id", "client_id", "trade_date").contains(colName)) {
                terminationsAggDf = terminationsAggDf.withColumnRenamed(colName, "term_" + colName);
            }
        }

        // 5. Bring it back to Parent Orders
        Dataset<Row> enrichedOrdersDf = parentOrdersDf.as("p")
            .join(fillsAggDf.as("f"), functions.col("p.order_id").equalTo(functions.col("f.order_id")).and(functions.col("p.client_id").equalTo(functions.col("f.client_id"))), "left")
            .join(acksAggDf.as("a"), functions.col("p.order_id").equalTo(functions.col("a.order_id")).and(functions.col("p.client_id").equalTo(functions.col("a.client_id"))), "left")
            .join(terminationsAggDf.as("t"), functions.col("p.order_id").equalTo(functions.col("t.order_id")).and(functions.col("p.client_id").equalTo(functions.col("t.client_id"))), "left")
            .join(closeFillPriceDf.as("c"), functions.col("p.order_id").equalTo(functions.col("c.close_order_id")).and(functions.col("p.client_id").equalTo(functions.col("c.close_client_id"))), "left")
            .drop(functions.col("f.order_id"), functions.col("f.client_id"), functions.col("a.order_id"), functions.col("a.client_id"), functions.col("t.order_id"), functions.col("t.client_id"), functions.col("c.close_order_id"), functions.col("c.close_client_id"));

        String utcTimeMarketOpen = this.localToUtcTime(9, 30, runDate, "America/New_York");
        String utcTimeMarketClose = this.localToUtcTime(16, 0, runDate, "America/New_York");

        // Establish effective operating window
        enrichedOrdersDf = enrichedOrdersDf
            .withColumn("EffectiveStartTime", functions.least(functions.greatest(functions.col("ack_AckStartTime"), functions.lit(utcTimeMarketOpen).cast("timestamp")), functions.col("fill_FillStartTime")))
            .withColumn("EffectiveEndTime", functions.least(functions.col("term_ExecutionEndTime"), functions.lit(utcTimeMarketClose).cast("timestamp")));

        // 6. Market Data Tick Metrics (complex time-based joins)
        Dataset<Row> quotesDf = spark.read().parquet("gs://level1_quotes/");

        quotesDf = quotesDf.repartition(functions.col("ticker")).sortWithinPartitions("quote_timestamp");
        enrichedOrdersDf = enrichedOrdersDf.repartition(functions.col("ticker")).sortWithinPartitions("EffectiveStartTime");

        quotesDf = quotesDf.withColumn("quote_pk", functions.monotonically_increasing_id());
        enrichedOrdersDf = enrichedOrdersDf.withColumn("order_pk", functions.monotonically_increasing_id());

        // Build Window buffers for Quote lookups
        enrichedOrdersDf = enrichedOrdersDf
            .withColumn("Start_lower", functions.expr("TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE)")) // BigQuery equivalent for 'interval 10 minutes'
            .withColumn("End_lower", functions.expr("TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE)"))   // BigQuery equivalent for 'interval 10 minutes'
            .withColumn("End_plus1", functions.expr("TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE)"))    // BigQuery equivalent for 'interval 1 minutes'
            .withColumn("End_plus1_lower", functions.expr("TIMESTAMP_SUB(End_plus1, INTERVAL 10 MINUTE)"))     // BigQuery equivalent for 'interval 10 minutes'
            .withColumn("End_plus5", functions.expr("TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE)"))    // BigQuery equivalent for 'interval 5 minutes'
            .withColumn("End_plus5_lower", functions.expr("TIMESTAMP_SUB(End_plus5, INTERVAL 10 MINUTE)"));    // BigQuery equivalent for 'interval 10 minutes'

        // Define a helper method to reuse logic for looking up the nearest quote
        // This is a conversion of a Python nested function/closure to a private Java method.
        Dataset<Row> openQuotes = findNearestQuote(enrichedOrdersDf, quotesDf, "EffectiveStartTime", "Start_lower", "start");
        Dataset<Row> endQuotes = findNearestQuote(enrichedOrdersDf, quotesDf, "EffectiveEndTime", "End_lower", "end");
        Dataset<Row> end1mQuotes = findNearestQuote(enrichedOrdersDf, quotesDf, "End_plus1", "End_plus1_lower", "end_plus1");

        // 7. Core VWAP and Financial Performance calculations
        Dataset<Row> tradesDf = spark.read().parquet("gs://market_trades/");

        // VWAP during order existence
        Dataset<Row> vwapDf = enrichedOrdersDf
            .join(tradesDf,
                enrichedOrdersDf.col("ticker").equalTo(tradesDf.col("ticker"))
                .and(tradesDf.col("trade_timestamp").geq(enrichedOrdersDf.col("EffectiveStartTime")))
                .and(tradesDf.col("trade_timestamp").leq(enrichedOrdersDf.col("EffectiveEndTime"))),
                "inner")
            .groupBy("order_id", "client_id")
            .agg(
                functions.sum(tradesDf.col("trade_size")).alias("market_interval_volume"),
                functions.callUDF("`divide`", functions.sum(tradesDf.col("trade_price").multiply(tradesDf.col("trade_size"))), functions.sum(tradesDf.col("trade_size"))).alias("market_interval_vwap")
            );

        // Join everything back
        Dataset<Row> finalDf = enrichedOrdersDf
            .join(vwapDf, new String[]{"order_id", "client_id"}, "left")
            // Selectively bringing fields from quote buffers
            .join(openQuotes.select("order_pk", "start_best_bid", "start_best_ask", "start_quote_timestamp"), "order_pk", "left")
            .join(endQuotes.select("order_pk", "end_best_bid", "end_best_ask"), "order_pk", "left")
            .join(end1mQuotes.select("order_pk", "end_plus1_best_bid", "end_plus1_best_ask"), "order_pk", "left");

        // Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        finalDf = finalDf.withColumn("arrival_mid_price", functions.col("start_best_bid").plus(functions.col("start_best_ask")).divide(2));

        // Slippage from VWAP
        finalDf = finalDf.withColumn("slippage_from_vwap_bps",
            functions.when(functions.col("side").equalTo("BUY"), (functions.col("fill_AverageExecutionPrice").minus(functions.col("market_interval_vwap"))).divide(functions.col("market_interval_vwap")).multiply(10000))
            .when(functions.col("side").equalTo("SELL"), (functions.col("market_interval_vwap").minus(functions.col("fill_AverageExecutionPrice"))).divide(functions.col("market_interval_vwap")).multiply(10000))
        );

        // Slippage from Arrival Mid (Implementation Shortfall)
        finalDf = finalDf.withColumn("implementation_shortfall_pl",
            functions.when(functions.col("side").equalTo("BUY"), (functions.col("arrival_mid_price").minus(functions.col("fill_AverageExecutionPrice"))).multiply(functions.col("fill_TotalSharesExecuted")))
            .when(functions.col("side").equalTo("SELL"), (functions.col("fill_AverageExecutionPrice").minus(functions.col("arrival_mid_price"))).multiply(functions.col("fill_TotalSharesExecuted")))
        );

        // Momentum calculations post-trade
        finalDf = finalDf.withColumn("post_trade_1m_momentum",
            functions.when(functions.col("side").equalTo("BUY"), (functions.col("end_plus1_best_bid").plus(functions.col("end_plus1_best_ask")).divide(2)).minus(functions.col("end_best_bid").plus(functions.col("end_best_ask")).divide(2)).multiply(functions.col("fill_TotalSharesExecuted")))
            .when(functions.col("side").equalTo("SELL"), (functions.col("end_best_bid").plus(functions.col("end_best_ask")).divide(2)).minus(functions.col("end_plus1_best_bid").plus(functions.col("end_plus1_best_ask")).divide(2)).multiply(functions.col("fill_TotalSharesExecuted")))
        );

        finalDf = finalDf.withColumn("opportunity_cost_pl",
            functions.when(functions.col("side").equalTo("BUY"), (functions.col("requested_shares").minus(functions.col("fill_TotalSharesExecuted"))).multiply(functions.col("fill_AverageExecutionPrice").minus(functions.col("closing_price"))))
            .when(functions.col("side").equalTo("SELL"), (functions.col("requested_shares").minus(functions.col("fill_TotalSharesExecuted"))).multiply(functions.col("closing_price").minus(functions.col("fill_AverageExecutionPrice"))))
        );

        finalDf.write().mode("overwrite").parquet(String.format("gs://trading_analytics/run_date=%s", runDate));
        System.out.println("Performance analysis complete.");
    }

    // Helper method corresponding to the Python nested function 'find_nearest_quote'
    private Dataset<Row> findNearestQuote(Dataset<Row> ordersDf, Dataset<Row> quotesDf, String targetTimeCol, String lowerBoundCol, String prefix) {
        Column joinCond = ordersDf.col("ticker").equalTo(quotesDf.col("ticker"))
            .and(functions.col("quote_timestamp").between(functions.col(lowerBoundCol), functions.col(targetTimeCol)));

        Dataset<Row> joined = ordersDf.join(quotesDf, joinCond, "inner")
            .hint("merge") // Spark hint
            .withColumn("time_diff", functions.abs(functions.col(targetTimeCol).minus(functions.col("quote_timestamp"))));

        WindowSpec windowSpec = Window.partitionBy("order_pk").orderBy("time_diff");
        Dataset<Row> nearest = joined
            .withColumn("rn", functions.row_number().over(windowSpec))
            .filter(functions.col("rn").equalTo(1))
            .drop("rn", "time_diff", "Start_lower", "End_lower", "End_plus1", "End_plus1_lower", "End_plus5", "End_plus5_lower", quotesDf.col("ticker")); // QuotesDf.col("ticker") for dropping

        // rename quote columns uniquely
        String[] colsToRename = {"quote_timestamp", "best_bid", "best_ask"};
        for (String c : colsToRename) {
            nearest = nearest.withColumnRenamed(c, String.format("%s_%s", prefix, c));
        }
        return nearest;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: AlgorithmicTradingPerformance <run_date>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("AlgorithmicTradingPerformance").getOrCreate();
        AlgorithmicTradingPerformance p = new AlgorithmicTradingPerformance();
        p.executePipeline(spark, args[0]);
        spark.stop();
    }
}
