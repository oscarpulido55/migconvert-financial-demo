from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime
import pytz
import sys

class AlgorithmicTradingPerformance:
    """
    Simulates high-frequency trading performance evaluation.
    It joins order logs (acks, fills, closures) with market data ticks to evaluate
    slippage (PL), opportunity costs, and VWAP (Volume-Weighted Average Price) differences.
    """

    def local_to_utc_time(self, hour, minute, date_str, tz='America/New_York'):
        local_tz = pytz.timezone(tz)
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        local_datetime = local_tz.localize(
            datetime.datetime.combine(date_obj, datetime.time(hour, minute))
        )
        return local_datetime.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

    def execute_pipeline(self, spark: SparkSession, run_date: str):

        # 1. Load Core Datasets
        # Converted from reading Parquet files on HDFS to reading from BigQuery tables.
        # Replace 'PROJECT_ID' and 'DATASET_ID' with your actual Google Cloud Project ID and BigQuery Dataset ID.
        # Ensure Spark-BigQuery connector is configured (e.g., via spark.jars.packages in SparkSession config).
        all_order_events_df = spark.read.format("bigquery").option("table", "PROJECT_ID.DATASET_ID.trading_events_base").load()
        parent_orders_df = spark.read.format("bigquery").option("table", "PROJECT_ID.DATASET_ID.parent_orders").load()

        # 2. Separate Event Stream into Fills
        # Anonymized protocol filtering conceptually representing status flags
        fills_df = all_order_events_df.filter(
            ((F.col("protocol_version") == "V1") & (F.col("status").isin("FILLED", "PARTIAL")) & (F.col("event_type") == "TRADE"))
            | ((F.col("protocol_version") >= "V2") & (F.col("event_type") == "FILL"))
        )

        # Get the closing fill price per order
        close_fill_price_df = (fills_df
            .withColumn("rn", F.row_number().over(Window.partitionBy("order_id", "client_id").orderBy("event_timestamp")))
            .filter(F.col("rn") == 1)
            .select(
                F.col("order_id").alias("close_order_id"),
                F.col("client_id").alias("close_client_id"),
                F.col("last_exec_price").alias("closing_price"),
                F.col("last_exec_qty").alias("closing_qty")
            )
            .drop("rn"))

        # Aggregate fills per order
        fills_agg_df = (fills_df
            .groupBy("order_id", "client_id", "trade_date")
            .agg(
                F.least(F.min("event_timestamp"), F.min("routing_timestamp")).alias("FillStartTime"),
                F.min("event_timestamp").alias("FirstFillTime"),
                F.sum("last_exec_qty").alias("TotalSharesExecuted"),
                F.count("last_exec_qty").alias("NumberOfFills"),
                (F.sum(F.col("last_exec_qty") * F.col("last_exec_price")) / F.sum("last_exec_qty")).alias("AverageExecutionPrice"),
                F.sum(F.col("last_exec_qty") * F.col("last_exec_price")).alias("TotalMarketValueExecuted"),
            ))

        # Prefix columns for joins
        for col_name in fills_agg_df.columns:
            if col_name not in ['order_id', 'client_id', 'trade_date']:
                fills_agg_df = fills_agg_df.withColumnRenamed(col_name, "fill_" + col_name)

        # 3. Execution Acknowledgements
        acks_df = all_order_events_df.filter(F.col("status").isin("NEW", "REPLACED") & (F.col("event_type") == "ACK"))
        acks_agg_df = (acks_df
            .groupBy("order_id", "client_id", "trade_date")
            .agg(F.least(F.min("event_timestamp"), F.min("routing_timestamp")).alias("AckStartTime")))

        for col_name in acks_agg_df.columns:
            if col_name not in ['order_id', 'client_id', 'trade_date']:
                acks_agg_df = acks_agg_df.withColumnRenamed(col_name, "ack_" + col_name)

        # 4. Execution Terminations
        terminations_df = all_order_events_df.filter(F.col("status").isin("CANCELED", "DONE_FOR_DAY", "EXPIRED", "REJECTED"))
        terminations_agg_df = (terminations_df
            .groupBy("order_id", "client_id", "trade_date")
            .agg(F.greatest(F.max("event_timestamp"), F.max("routing_timestamp")).alias("ExecutionEndTime")))

        for col_name in terminations_agg_df.columns:
            if col_name not in ['order_id', 'client_id', 'trade_date']:
                terminations_agg_df = terminations_agg_df.withColumnRenamed(col_name, "term_" + col_name)

        # 5. Bring it back to Parent Orders
        enriched_orders_df = (
            parent_orders_df.alias("p")
            .join(fills_agg_df.alias("f"), (F.col("p.order_id") == F.col("f.order_id")) & (F.col("p.client_id") == F.col("f.client_id")), "left")
            .join(acks_agg_df.alias("a"), (F.col("p.order_id") == F.col("a.order_id")) & (F.col("p.client_id") == F.col("a.client_id")), "left")
            .join(terminations_agg_df.alias("t"), (F.col("p.order_id") == F.col("t.order_id")) & (F.col("p.client_id") == F.col("t.client_id")), "left")
            .join(close_fill_price_df.alias("c"), (F.col("p.order_id") == F.col("c.close_order_id")) & (F.col("p.client_id") == F.col("c.close_client_id")), "left")
        ).drop(F.col("f.order_id"), F.col("f.client_id"), F.col("a.order_id"), F.col("a.client_id"), F.col("t.order_id"), F.col("t.client_id"), F.col("c.close_order_id"), F.col("c.close_client_id"))

        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # Establish effective operating window
        enriched_orders_df = (
            enriched_orders_df
            .withColumn("EffectiveStartTime", F.least(F.greatest(F.col("ack_AckStartTime"), F.lit(utc_time_market_open).cast("timestamp")), F.col("fill_FillStartTime")))
            .withColumn("EffectiveEndTime", F.least(F.col("term_ExecutionEndTime"), F.lit(utc_time_market_close).cast("timestamp")))
        )

        # 6. Market Data Tick Metrics (complex time-based joins)
        # Converted from reading Parquet files on HDFS to reading from BigQuery tables.
        # Replace 'PROJECT_ID' and 'DATASET_ID' with your actual Google Cloud Project ID and BigQuery Dataset ID.
        quotes_df = spark.read.format("bigquery").option("table", "PROJECT_ID.DATASET_ID.level1_quotes").load()

        quotes_df = quotes_df.repartition("ticker").sortWithinPartitions("quote_timestamp")
        enriched_orders_df = enriched_orders_df.repartition("ticker").sortWithinPartitions("EffectiveStartTime")

        quotes_df = quotes_df.withColumn("quote_pk", F.monotonically_increasing_id())
        enriched_orders_df = enriched_orders_df.withColumn("order_pk", F.monotonically_increasing_id())

        # Build Window buffers for Quote lookups
        enriched_orders_df = (enriched_orders_df
            .withColumn("Start_lower", F.expr(f"EffectiveStartTime - interval 10 minutes"))
            .withColumn("End_lower", F.expr(f"EffectiveEndTime - interval 10 minutes"))
            .withColumn("End_plus1", F.expr(f"EffectiveEndTime + interval 1 minutes"))
            .withColumn("End_plus1_lower", F.expr(f"End_plus1 - interval 10 minutes"))
            .withColumn("End_plus5", F.expr(f"EffectiveEndTime + interval 5 minutes"))
            .withColumn("End_plus5_lower", F.expr(f"End_plus5 - interval 10 minutes"))
        )

        # Define a closure to reuse logic for looking up the nearest quote
        def find_nearest_quote(orders_df, quotes_df, target_time_col, lower_bound_col, prefix):
            join_cond = (orders_df["ticker"] == quotes_df["ticker"]) & (F.col("quote_timestamp").between(F.col(lower_bound_col), F.col(target_time_col)))

            # Removed Spark-specific hint("merge") as it's not a BigQuery construct and PySpark handles it generically.
            joined = (orders_df.join(quotes_df, join_cond, how="inner")
                        .withColumn("time_diff", F.abs(F.col(target_time_col) - F.col("quote_timestamp"))))

            nearest = (joined
                        .withColumn("rn", F.row_number().over(Window.partitionBy("order_pk").orderBy("time_diff")))
                        .filter(F.col("rn") == 1)
                        .drop("rn", "time_diff", "Start_lower", "End_lower", "End_plus1", "End_plus1_lower", "End_plus5", "End_plus5_lower", quotes_df["ticker"]))

            # rename quote columns uniquely
            for c in ["quote_timestamp", "best_bid", "best_ask"]:
                nearest = nearest.withColumnRenamed(c, f"{prefix}_{c}")
            return nearest

        # Perform lookups
        open_quotes = find_nearest_quote(enriched_orders_df, quotes_df, "EffectiveStartTime", "Start_lower", "start")
        end_quotes = find_nearest_quote(enriched_orders_df, quotes_df, "EffectiveEndTime", "End_lower", "end")
        end_1m_quotes = find_nearest_quote(enriched_orders_df, quotes_df, "End_plus1", "End_plus1_lower", "end_plus1")

        # 7. Core VWAP and Financial Performance calculations
        # Converted from reading Parquet files on HDFS to reading from BigQuery tables.
        # Replace 'PROJECT_ID' and 'DATASET_ID' with your actual Google Cloud Project ID and BigQuery Dataset ID.
        trades_df = spark.read.format("bigquery").option("table", "PROJECT_ID.DATASET_ID.market_trades").load()

        # VWAP during order existence
        vwap_df = (enriched_orders_df
            .join(trades_df, (enriched_orders_df["ticker"] == trades_df["ticker"]) &
                             (trades_df["trade_timestamp"] >= enriched_orders_df["EffectiveStartTime"]) &
                             (trades_df["trade_timestamp"] <= enriched_orders_df["EffectiveEndTime"]), "inner")
            .groupBy("order_id", "client_id")
            .agg(
                F.sum(trades_df["trade_size"]).alias("market_interval_volume"),
                (F.sum(trades_df["trade_price"] * trades_df["trade_size"]) / F.sum(trades_df["trade_size"])).alias("market_interval_vwap")
            ))

        # Join everything back
        final_df = (enriched_orders_df
            .join(vwap_df, ["order_id", "client_id"], "left")
            # Selectively bringing fields from quote buffers
            .join(open_quotes.select("order_pk", "start_best_bid", "start_best_ask", "start_quote_timestamp"), "order_pk", "left")
            .join(end_quotes.select("order_pk", "end_best_bid", "end_best_ask"), "order_pk", "left")
            .join(end_1m_quotes.select("order_pk", "end_plus1_best_bid", "end_plus1_best_ask"), "order_pk", "left")
        )

        # Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        final_df = final_df.withColumn("arrival_mid_price", (F.col("start_best_bid") + F.col("start_best_ask")) / 2)

        # Slippage from VWAP
        final_df = final_df.withColumn("slippage_from_vwap_bps",
            F.when(F.col("side") == "BUY", ((F.col("fill_AverageExecutionPrice") - F.col("market_interval_vwap")) / F.col("market_interval_vwap")) * 10000)
             .when(F.col("side") == "SELL", ((F.col("market_interval_vwap") - F.col("fill_AverageExecutionPrice")) / F.col("market_interval_vwap")) * 10000)
        )

        # Slippage from Arrival Mid (Implementation Shortfall)
        final_df = final_df.withColumn("implementation_shortfall_pl",
            F.when(F.col("side") == "BUY", (F.col("arrival_mid_price") - F.col("fill_AverageExecutionPrice")) * F.col("fill_TotalSharesExecuted"))
             .when(F.col("side") == "SELL", (F.col("fill_AverageExecutionPrice") - F.col("arrival_mid_price")) * F.col("fill_TotalSharesExecuted"))
        )

        # Momentum calculations post-trade
        final_df = final_df.withColumn("post_trade_1m_momentum",
            F.when(F.col("side") == "BUY", (((F.col("end_plus1_best_bid") + F.col("end_plus1_best_ask")) / 2) - ((F.col("end_best_bid") + F.col("end_best_ask")) / 2)) * F.col("fill_TotalSharesExecuted"))
             .when(F.col("side") == "SELL", (((F.col("end_best_bid") + F.col("end_best_ask")) / 2) - ((F.col("end_plus1_best_bid") + F.col("end_plus1_best_ask")) / 2)) * F.col("fill_TotalSharesExecuted"))
        )

        final_df = final_df.withColumn("opportunity_cost_pl",
            F.when(F.col("side") == "BUY", (F.col("requested_shares") - F.col("fill_TotalSharesExecuted")) * (F.col("fill_AverageExecutionPrice") - F.col("closing_price")))
             .when(F.col("side") == "SELL", (F.col("requested_shares") - F.col("fill_TotalSharesExecuted")) * (F.col("closing_price") - F.col("fill_AverageExecutionPrice")))
        )

        # Converted from writing Parquet to HDFS to writing to a BigQuery table.
        # BigQuery table names cannot contain hyphens, so run_date format is adjusted.
        # Replace 'PROJECT_ID', 'DATASET_ID', and 'your-gcs-bucket-for-temp-files' with actual values.
        # The 'temporaryGcsBucket' option specifies a GCS bucket for Spark to use for temporary data during the write operation.
        final_df.write.format("bigquery").option("table", f"PROJECT_ID.DATASET_ID.trading_analytics_{run_date.replace('-', '_')}").option("temporaryGcsBucket", "your-gcs-bucket-for-temp-files").mode("overwrite").save()
        print("Performance analysis complete.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        sys.exit(1)

    # When running with BigQuery, ensure the Spark-BigQuery connector JAR is available on the classpath.
    # This is typically configured in your spark-submit command (e.g., --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.29.1)
    # or in the SparkSession.builder.config if not using spark-submit --packages.
    spark = SparkSession.builder.appName("AlgorithmicTradingPerformance").getOrCreate()
    p = AlgorithmicTradingPerformance()
    p.execute_pipeline(spark, sys.argv[1])
    spark.stop()
