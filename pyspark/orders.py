from pyspark.sql import SparkSession, DataFrame
import argparse
from typing import Optional, Sequence
import pyspark.sql.functions as F
from pipelines.utils import sparkschemas, sparkutils
from pyspark.sql.window import Window
import datetime
import pytz

class TransformOrders():

    def nyc_time_to_utc_time(self, hour, minute, date_str):
        nyc_timezone = pytz.timezone('America/New_York')
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

        nyc_datetime = nyc_timezone.localize(
            datetime.datetime.combine(date_obj, datetime.time(hour, minute))
        )
        utc_datetime = nyc_datetime.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
        return utc_datetime

    def run(self, spark: SparkSession):

        # all execution events (orders base)
        all_executions_df = spark.read.parquet("hdfs://test_all_executions/")

        # orders (necessary for Algorithm and Tactic only)
        orders_df = spark.read.parquet("hdfs://orders_df/")

        # Execution FILLs
        fill_executions_df = (all_executions_df
                             .filter(
                                ((F.col("beginstring") == "FIX.4.0") & (F.col("ordstatus").isin("1", "2")) & (F.col("exectranstype") == "0"))
                                | ((F.col("beginstring").isin("FIX.4.1", "FIX.4.2"))  & (F.col("exectype").isin("1", "2")) & (F.col("exectranstype") == "0"))
                                | ((F.col("beginstring") >= "FIX.4.3") & (F.col("exectype") == "F"))
                            ))

        # close price - per order
        close_fill_price_df = (fill_executions_df
                          .withColumn("rn", F.row_number().over(Window.partitionBy("cl_ordid", "cust_id").orderBy("transacttime")))
                          .filter(F.col("rn") == 1)
                          .select(F.col("cl_ordid").alias("close_cl_ordid"), F.col("cust_id").alias("close_cust_id"), F.col("lastpx").alias("close_price"), F.col("lastqty").alias("close_qty"))
                          .drop("rn"))

        fill_executions_df = (fill_executions_df
                             .groupBy("cl_ordid", "cust_id", "trade_date")
                             .agg(F.least(F.min("transacttime"), F.min("sending_time")).alias("FillStartTime"),
                                F.min("transacttime").alias("FirstFillTime"),
                                F.sum("lastqty").alias("SharesExecuted"),
                                F.count("lastqty").alias("NumFills"),
                                (F.sum(F.col("lastqty") * F.col("LastPX")) / F.sum("lastqty")).alias("AveragePrice"),
                                F.sum(F.col("lastqty") * F.col("LastPX")).alias("MarketValueExecuted"),
                        ))
        for col_name in fill_executions_df.columns:
            fill_executions_df = fill_executions_df.withColumnRenamed(col_name, "fill_" + col_name)

        # Execution ACKs
        ack_executions_df = (all_executions_df
                            .filter(
                            ((F.col("ordstatus").isin("0", "5"))& (F.col("exectranstype") == "0")& (F.col("beginstring") == "FIX.4.0"))
                            | ((F.col("exectype").isin("0", "5"))& (F.col("exectranstype") == "0")& (F.col("beginstring").isin("FIX.4.1", "FIX.4.2")))
                            | ((F.col("exectype").isin("0", "5")) & (F.col("beginstring") >= "FIX4.3"))
                        ))
        ack_executions_df = (ack_executions_df
                             .groupBy("cl_ordid", "cust_id", "trade_date")
                             .agg(F.least(F.min("transacttime"), F.min("sending_time")).alias("AcKStartTime")))
        for col_name in ack_executions_df.columns:
            ack_executions_df = ack_executions_df.withColumnRenamed(col_name, "ack_" + col_name)

        # Execution ENDs
        end_executions_df = all_executions_df.filter(F.col("ordstatus").isin("2", "3", "4", "5", "8", "C"))
        end_executions_df = (end_executions_df
                             .groupBy("cl_ordid", "cust_id", "trade_date")
                             .agg(F.greatest(F.max("transacttime"), F.max("sending_time")).alias("exEndtime")))
        for col_name in end_executions_df.columns:
            end_executions_df = end_executions_df.withColumnRenamed(col_name, "end_" + col_name)

        # Orders
        filtered_nyfix_orders_df = (
            orders_df.join(fill_executions_df,
                (orders_df.cl_ordid == fill_executions_df.fill_cl_ordid)
                & (orders_df.cust_id == fill_executions_df.fill_cust_id),
                "left",
            )
            .join(ack_executions_df,
                (orders_df.cl_ordid == ack_executions_df.ack_cl_ordid)
                & (orders_df.cust_id == ack_executions_df.ack_cust_id),
                "left",
            )
            .join(end_executions_df,
                (orders_df.cl_ordid == end_executions_df.end_cl_ordid)
                & (orders_df.cust_id == end_executions_df.end_cust_id),
                "left",
            )
            .join(close_fill_price_df,
                (orders_df.cl_ordid == close_fill_price_df.close_cl_ordid)
                & (orders_df.cust_id == close_fill_price_df.close_cust_id),
                "left",
            )
        )

        utc_time_market_open_time = self.nyc_time_to_utc_time(9, 30, self.pipeline_options.start_date)
        utc_time_market_close_time = self.nyc_time_to_utc_time(16, 00, self.pipeline_options.start_date)

        filtered_nyfix_orders_df = (
            filtered_nyfix_orders_df
            .withColumn("StartTime", F.least(F.greatest(F.col("ack_AcKStartTime"), F.lit(utc_time_market_open_time).cast("timestamp")), F.col("fill_FillStartTime")))
            .withColumn("EndTime", F.least(F.col("end_exEndtime"), F.lit(utc_time_market_close_time).cast("timestamp")))
        )

        # Quotes
        quotes_df = spark.read.parquet("hdfs://quotes_df/")

        # Partitioning and Sorting StartTime:
        quotes_df = (quotes_df
            .repartition("symbol")
            .sortWithinPartitions("event_timestamp"))
        filtered_nyfix_orders_df = (filtered_nyfix_orders_df
            .repartition("symbol")
            .sortWithinPartitions("StartTime"))

        # adding ID
        quotes_df = quotes_df.withColumn("bmll_id", F.monotonically_increasing_id())
        filtered_nyfix_orders_df = filtered_nyfix_orders_df.withColumn("nyfix_id", F.monotonically_increasing_id())

        filtered_nyfix_orders_df = (filtered_nyfix_orders_df
                                    .withColumn("StartTime_lower", F.expr(f"StartTime - interval 10 minutes"))
                                    .withColumn("StartTime_upper", F.expr(f"StartTime + interval 10 minutes"))
                                    .withColumn("EndTime_lower", F.expr(f"EndTime - interval 10 minutes"))
                                    .withColumn("at_end_plus1", F.expr(f"EndTime + interval 1 minutes"))
                                    .withColumn("at_end_plus1_lower", F.expr(f"at_end_plus1 - interval 10 minutes"))
                                    .withColumn("at_end_plus5", F.expr(f"EndTime + interval 5 minutes"))
                                    .withColumn("at_end_plus5_lower", F.expr(f"at_end_plus5 - interval 10 minutes"))
                                    .withColumn("at_end_plus60", F.expr(f"EndTime + interval 60 minutes"))
                                    .withColumn("at_end_plus60_lower", F.expr(f"at_end_plus60 - interval 10 minutes")))

        # Join at start
        condition_at_open = (filtered_nyfix_orders_df.filter(F.col("StartTime") == F.lit(utc_time_market_open_time).cast("timestamp"))
                             .join(quotes_df, (filtered_nyfix_orders_df["symbol"] == quotes_df["symbol"]) &
                                  (F.col("event_timestamp").between(F.col("StartTime"), F.col("StartTime_upper")))
                                   , how="inner")
                             .hint("merge")
                             .drop(quotes_df["symbol"])
                             .withColumn("time_diff", F.abs(F.col("StartTime") - F.col("event_timestamp")))
                             )

        condition_not_at_open = (filtered_nyfix_orders_df.filter(F.col("StartTime") != F.lit(utc_time_market_open_time).cast("timestamp"))
                                .join(quotes_df, (filtered_nyfix_orders_df["symbol"] == quotes_df["symbol"]) &
                                    (F.col("event_timestamp").between(F.col("StartTime_lower"), F.col("StartTime")))
                                    ,how = "inner")
                                .hint("merge")
                                .drop(quotes_df["symbol"])
                                .withColumn("time_diff", F.abs(F.col("StartTime") - F.col("event_timestamp")))
                                )

        merged_at_start_df = condition_at_open.unionByName(condition_not_at_open)

        merged_at_start_df = (merged_at_start_df
                    .withColumn("row_number", F.row_number().over(Window.partitionBy("nyfix_id").orderBy("time_diff")))
                    .filter(F.col("row_number") == 1)
                    .drop(F.col("row_number"))
                    .drop(F.col("time_diff")))
        for col_name in ["event_timestamp", "best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size", "cl_ordid", "cust_id"]:
            merged_at_start_df = merged_at_start_df.withColumnRenamed(col_name, f"at_start_{col_name}")

        # Join at end
        merged_at_end_df = (filtered_nyfix_orders_df
                     .join(quotes_df,
                           (filtered_nyfix_orders_df["symbol"] == quotes_df["symbol"]) &
                           (F.col("event_timestamp").between(F.col("EndTime_lower"), F.col("EndTime")))
                           , how="inner")
                     .hint("merge")
                     .drop("EndTime_lower")
                     .drop(quotes_df["symbol"])
                     .withColumn("time_diff", F.abs(F.col("EndTime") - F.col("event_timestamp")))
                     )
        merged_at_end_df = (merged_at_end_df
                    .withColumn("row_number", F.row_number().over(Window.partitionBy("nyfix_id").orderBy("time_diff")))
                    .filter(F.col("row_number") == 1)
                    .drop(F.col("row_number"))
                    .drop(F.col("time_diff")))
        for col_name in ["event_timestamp", "best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size", "cl_ordid", "cust_id"]:
            merged_at_end_df = merged_at_end_df.withColumnRenamed(col_name, f"at_end_{col_name}")

        # Join at end +1 min
        merged_at_end_plus1_df = (filtered_nyfix_orders_df
                     .join(quotes_df,
                           (filtered_nyfix_orders_df["symbol"] == quotes_df["symbol"]) &
                           (F.col("event_timestamp").between(F.col("at_end_plus1_lower"), F.col("at_end_plus1")))
                           ,how="inner")
                     .hint("merge")
                     .drop("at_end_plus1_lower")
                     .drop(quotes_df["symbol"])
                     .withColumn("time_diff", F.abs(F.col("at_end_plus1") - F.col("event_timestamp")))
                     )
        merged_at_end_plus1_df = (merged_at_end_plus1_df
                    .withColumn("row_number", F.row_number().over(Window.partitionBy("nyfix_id").orderBy("time_diff")))
                    .filter(F.col("row_number") == 1)
                    .drop(F.col("row_number"))
                    .drop(F.col("time_diff")))
        for col_name in ["event_timestamp", "best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size", "cl_ordid", "cust_id"]:
            merged_at_end_plus1_df = merged_at_end_plus1_df.withColumnRenamed(col_name, f"at_end_plus1_{col_name}")

        # Join at end +5 min
        merged_at_end_plus5_df = (filtered_nyfix_orders_df
                     .join(quotes_df,
                           (filtered_nyfix_orders_df["symbol"] == quotes_df["symbol"]) &
                           (F.col("event_timestamp").between(F.col("at_end_plus5_lower"), F.col("at_end_plus5")))
                           ,how="inner")
                     .hint("merge")
                     .drop("at_end_plus5_lower")
                     .drop(quotes_df["symbol"])
                     .withColumn("time_diff", F.abs(F.col("at_end_plus5") - F.col("event_timestamp")))
                     )
        merged_at_end_plus5_df = (merged_at_end_plus5_df
                    .withColumn("row_number", F.row_number().over(Window.partitionBy("nyfix_id").orderBy("time_diff")))
                    .filter(F.col("row_number") == 1)
                    .drop(F.col("row_number"))
                    .drop(F.col("time_diff")))
        for col_name in ["event_timestamp", "best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size", "cl_ordid", "cust_id"]:
            merged_at_end_plus5_df = merged_at_end_plus5_df.withColumnRenamed(col_name, f"at_end_plus5_{col_name}")

        # Join at end +60 min
        merged_at_end_plus60_df = (filtered_nyfix_orders_df
                                  .join(quotes_df,
                                        (filtered_nyfix_orders_df["symbol"] == quotes_df["symbol"]) &
                                        (F.col("event_timestamp").between(F.col("at_end_plus60_lower"), F.col("at_end_plus60")))
                                        , how="inner")
                                  .hint("merge")
                                  .drop("at_end_plus60_lower")
                                  .drop(quotes_df["symbol"])
                                  .withColumn("time_diff", F.abs(F.col("at_end_plus60") - F.col("event_timestamp")))
                                  )
        merged_at_end_plus60_df = (merged_at_end_plus60_df
                                  .withColumn("row_number",
                                              F.row_number().over(Window.partitionBy("nyfix_id").orderBy("time_diff")))
                                  .filter(F.col("row_number") == 1)
                                  .drop(F.col("row_number"))
                                  .drop(F.col("time_diff")))
        for col_name in ["event_timestamp", "best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size", "cl_ordid", "cust_id"]:
            merged_at_end_plus60_df = merged_at_end_plus60_df.withColumnRenamed(col_name, f"at_end_plus60_{col_name}")

        # as of values join
        as_of_df = (filtered_nyfix_orders_df.join(merged_at_start_df,
                        on=[filtered_nyfix_orders_df.cl_ordid == merged_at_start_df.at_start_cl_ordid,
                            filtered_nyfix_orders_df.cust_id == merged_at_start_df.at_start_cust_id], how="left"))

        as_of_df = (as_of_df.join(merged_at_end_df,
                          on=[filtered_nyfix_orders_df.cl_ordid == merged_at_end_df.at_end_cl_ordid,
                              filtered_nyfix_orders_df.cust_id == merged_at_end_df.at_end_cust_id], how="left"))

        as_of_df = (as_of_df.join(merged_at_end_plus1_df,
                          on=[filtered_nyfix_orders_df.cl_ordid == merged_at_end_plus1_df.at_end_plus1_cl_ordid,
                              filtered_nyfix_orders_df.cust_id == merged_at_end_plus1_df.at_end_plus1_cust_id], how="left"))

        as_of_df = (as_of_df.join(merged_at_end_plus5_df,
                          on=[filtered_nyfix_orders_df.cl_ordid == merged_at_end_plus5_df.at_end_plus5_cl_ordid,
                              filtered_nyfix_orders_df.cust_id == merged_at_end_plus5_df.at_end_plus5_cust_id], how="left"))

        as_of_df = (as_of_df.join(merged_at_end_plus60_df,
                          on=[filtered_nyfix_orders_df.cl_ordid == merged_at_end_plus60_df.at_end_plus60_cl_ordid,
                              filtered_nyfix_orders_df.cust_id == merged_at_end_plus60_df.at_end_plus60_cust_id], how="left"))

        as_of_df = as_of_df.select(
            filtered_nyfix_orders_df["*"],
            merged_at_start_df["at_start_best_bid_price"],
            merged_at_start_df["at_start_best_ask_price"],
            merged_at_start_df["at_start_best_bid_size"],
            merged_at_start_df["at_start_best_ask_size"],
            merged_at_end_df["at_end_best_bid_price"],
            merged_at_end_df["at_end_best_ask_price"],
            merged_at_end_df["at_end_best_bid_size"],
            merged_at_end_df["at_end_best_ask_size"],
            merged_at_end_plus1_df["at_end_plus1_best_bid_price"],
            merged_at_end_plus1_df["at_end_plus1_best_ask_price"],
            merged_at_end_plus1_df["at_end_plus1_best_bid_size"],
            merged_at_end_plus1_df["at_end_plus1_best_ask_size"],
            merged_at_end_plus1_df["at_end_plus1_event_timestamp"],
            merged_at_end_plus5_df["at_end_plus5_best_bid_price"],
            merged_at_end_plus5_df["at_end_plus5_best_ask_price"],
            merged_at_end_plus5_df["at_end_plus5_best_bid_size"],
            merged_at_end_plus5_df["at_end_plus5_best_ask_size"],
            merged_at_end_plus60_df["at_end_plus60_best_bid_price"],
            merged_at_end_plus60_df["at_end_plus60_best_ask_price"],
            merged_at_end_plus60_df["at_end_plus60_best_bid_size"],
            merged_at_end_plus60_df["at_end_plus60_best_ask_size"],
        )

        # Trades
        trades_df = spark.read.parquet("hdfs://trades_df/")
        trades_df = trades_df.withColumn("volumeattime", F.sum("size").over(Window.partitionBy("symbol")))

        #btw start and end group
        btw_start_end_df = (filtered_nyfix_orders_df
                     .join(trades_df,
                           on=(filtered_nyfix_orders_df["symbol"] == trades_df["symbol"]) &
                              (trades_df["trade_timestamp"] >= filtered_nyfix_orders_df["StartTime"]) &
                              (trades_df["trade_timestamp"] <= filtered_nyfix_orders_df["EndTime"]),
                           how="inner"))
        btw_start_end_df = (btw_start_end_df
                     .groupBy("cl_ordid", "cust_id")
                     .agg(F.sum(F.coalesce(trades_df["size"], F.lit(0))).alias("interval_volume"),
                          (F.sum(trades_df["price"] * trades_df["size"]) / F.sum(trades_df["size"])).alias("vwap")
                          ))

        #limit btw_start_end
        filtered_nyfix_orders_df = (filtered_nyfix_orders_df
                .withColumn("limitorder", F.when(F.col("ordtype").isin({"2", "4", "7", "B"}), "Y").otherwise("N"))
                .filter((F.col("limitorder") == "Y")))

        buy_df = (filtered_nyfix_orders_df
                  .filter((F.col("side") == "Buy"))
                  .join(trades_df,
                        on=(filtered_nyfix_orders_df["symbol"] == trades_df["symbol"]) &
                           (trades_df["trade_timestamp"] >= filtered_nyfix_orders_df["StartTime"]) &
                           (trades_df["trade_timestamp"] <= filtered_nyfix_orders_df["EndTime"]) &
                           (trades_df["price"] > filtered_nyfix_orders_df["limitprice"]),
                        how="left")
                  .drop(trades_df["symbol"])
                  .groupBy("nyfix_id", "cl_ordid", "cust_id", "symbol", "side", "limitprice", "fill_AveragePrice", "fill_SharesExecuted")
                  .agg(
                    (F.sum(trades_df["size"] * trades_df["price"]) / F.sum(trades_df["size"]))
                        .alias("limit_adjusted_vwap"),
                    (F.sum(trades_df["size"]))
                        .alias("limit_adjusted_interval_volume"),
                    (F.sum(trades_df["size"] * trades_df["price"]))
                        .alias("mvlimitvwap"),
                    (((F.sum(trades_df["size"] * trades_df["price"]) / F.sum(trades_df["size"])) - F.col("fill_AveragePrice")) * F.col("fill_SharesExecuted"))
                        .alias("pllimitvwap")
                  ))
        sell_df = (filtered_nyfix_orders_df
                    .filter((F.col("side") == "Sell"))
                    .join(trades_df,
                          on=(filtered_nyfix_orders_df["symbol"] == trades_df["symbol"]) &
                             (trades_df["trade_timestamp"] >= filtered_nyfix_orders_df["StartTime"]) &
                             (trades_df["trade_timestamp"] <= filtered_nyfix_orders_df["EndTime"]) &
                             (filtered_nyfix_orders_df["limitprice"] > trades_df["price"]),
                          how="left")
                    .drop(trades_df["symbol"])
                    .groupBy("nyfix_id", "cl_ordid", "cust_id", "symbol", "side", "limitprice", "fill_AveragePrice", "fill_SharesExecuted")
                    .agg(
                        (F.sum(trades_df["size"] * trades_df["price"]) / F.sum(trades_df["size"]))
                            .alias("limit_adjusted_vwap"),
                        F.sum(trades_df["size"])
                            .alias("limit_adjusted_interval_volume"),
                        F.sum(trades_df["size"] * trades_df["price"])
                            .alias("mvlimitvwap"),
                        ((F.col("fill_AveragePrice") - (F.sum(trades_df["size"] * trades_df["price"]) / F.sum(trades_df["size"]))) * F.col("fill_SharesExecuted"))
                            .alias("pllimitvwap")
                    ))
        btw_start_end_limit_df = (buy_df.unionAll(sell_df)
                                  .drop(F.col("side"))
                                  .drop(F.col("fill_SharesExecuted"))
                                  .drop(F.col("fill_AveragePrice")))

        # prior_close_prices - per symbol
        prior_close_prices_df = spark.read.parquet("hdfs://prior_close_prices_df/")
        prior_close_prices_df = prior_close_prices_df.repartition("symbol")
        window_spec_close_prices = Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())
        prior_close_prices = (prior_close_prices_df
                              .withColumn("rn", F.row_number().over(window_spec_close_prices))
                              .filter(F.col("rn") == 1)
                              .select("symbol", F.col("close_price").alias("close_price_prior"))
                              .drop("rn")
                              .drop("trade_date"))

        # security characteristics
        security_char_df = spark.read.parquet("hdfs://security_char_df/")

        # security characteristics average daily volume
        average_daily_volume_df = spark.read.parquet("hdfs://average_daily_volume_df/")

        # open price - per symbol
        open_price_df = (trades_df
                         .filter(F.col("price").isNotNull())
                         .withColumn("rn", F.row_number().over(Window.partitionBy("symbol").orderBy("trade_timestamp")))
                         .filter(F.col("rn") == 1)
                         .select("symbol", F.col("price").alias("open_price"))
                         .drop("rn"))

        # final join
        joined_df = ((as_of_df.alias("a").join(btw_start_end_df.alias("b"),
                          on=[as_of_df.cl_ordid == btw_start_end_df.cl_ordid,
                              as_of_df.cust_id == btw_start_end_df.cust_id], how="left"))
                        .select("a.*", "b.interval_volume", "b.vwap"))

        joined_df = ((joined_df.alias("ab").join(btw_start_end_limit_df.alias("c"),
                            on=[joined_df.cl_ordid == btw_start_end_limit_df.cl_ordid,
                                joined_df.cust_id == btw_start_end_limit_df.cust_id], how="left"))
                        .select("ab.*", "c.limit_adjusted_vwap", "c.limit_adjusted_interval_volume", "c.mvlimitvwap", "c.pllimitvwap"))

        joined_df = (joined_df.join(open_price_df, on="symbol", how="left")
                     .join(security_char_df, on="symbol", how="left")
                     .join(prior_close_prices, on="symbol", how="left")
                     .join(average_daily_volume_df, on="symbol", how="left"))

        joined_df = joined_df.withColumn("fill_marketvalueordered", (F.col("SharesOrdered") * (F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2))
        joined_df = joined_df.withColumn("arrival_price", (F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2 )

        joined_df = (joined_df.withColumn("spread_capture",
                                          F.when(F.col("side") == "Buy",
                                                 (F.col("fill_SharesExecuted") * (F.col("at_end_best_ask_price") - F.col("close_price"))) / (F.col("fill_SharesExecuted") * F.col("close_price")))
                                          .when(F.col("side") == "Sell",
                                                (F.col("fill_SharesExecuted") * (F.col("close_price") - F.col("at_end_best_bid_price"))) / (F.col("fill_SharesExecuted") * F.col("close_price")))
                                          .otherwise(None)))

        joined_df = joined_df.withColumn("price_difference", (F.col("arrival_price") - F.col("close_price_prior")))

        joined_df = joined_df.withColumn("momentum_category",
                                         F.when(F.col("side") == "Buy",
                                                F.when(((F.col("price_difference") / F.col("close_price_prior")) * 100) > F.col("positive_momentum"), "Adverse")
                                                .when(((F.col("price_difference") / F.col("close_price_prior")) * 100) < F.col("negative_momentum"), "Favorable")
                                                .otherwise("Neutral"))
                                         .when(F.col("side") == "Sell",
                                               F.when(((F.col("price_difference") / F.col("close_price_prior")) * 100) > F.col("positive_momentum"), "Favorable")
                                               .when(((F.col("price_difference") / F.col("close_price_prior")) * 100) < F.col("negative_momentum"), "Adverse")
                                               .otherwise("Neutral"))
                                         .otherwise(None))

        joined_df = joined_df.withColumn("pleod",
                           F.when(F.col("side") == "Buy",
                                  F.col("fill_SharesExecuted") * (F.col("close_price") - as_of_df["fill_AveragePrice"]))
                           .when(F.col("side") == "Sell",
                                 F.col("fill_SharesExecuted") * (as_of_df["fill_AveragePrice"] - F.col("close_price")))
                           .otherwise(None))

        joined_df = joined_df.withColumn("mveod", F.col("fill_SharesExecuted") * as_of_df["fill_AveragePrice"])

        joined_df = joined_df.withColumn("pl1m",
                           F.when(F.col("side") == "Buy",
                                  (((F.col("at_end_plus1_best_bid_price") + F.col("at_end_plus1_best_ask_price")) / 2) - ((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2)) * F.col("fill_SharesExecuted"))
                           .when(F.col("side") == "Sell",
                                 (((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2) - ((F.col("at_end_plus1_best_bid_price") + F.col("at_end_plus1_best_ask_price")) / 2)) * F.col("fill_SharesExecuted"))
                           .otherwise(None))
        joined_df = joined_df.withColumn("mv1m", (((F.col("at_end_plus1_best_bid_price") + F.col("at_end_plus1_best_ask_price")) / 2) * F.col("fill_SharesExecuted")))

        joined_df = joined_df.withColumn("pl5m",
                           F.when(F.col("side") == "Buy",
                                  (((F.col("at_end_plus5_best_bid_price") + F.col("at_end_plus5_best_ask_price")) / 2) - ((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2)) * F.col("fill_SharesExecuted"))
                           .when(F.col("side") == "Sell",
                                 (((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2) - ((F.col("at_end_plus5_best_bid_price") + F.col("at_end_plus5_best_ask_price")) / 2)) * F.col("fill_SharesExecuted"))
                           .otherwise(None))
        joined_df = joined_df.withColumn("mv5m", (((F.col("at_end_plus5_best_bid_price") + F.col("at_end_plus5_best_ask_price")) / 2) * F.col("fill_SharesExecuted")))

        joined_df = joined_df.withColumn("pl60m",
                           F.when(F.col("side") == "Buy",
                                  (((F.col("at_end_plus60_best_bid_price") + F.col("at_end_plus60_best_ask_price")) / 2) - ((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2)) * F.col("fill_SharesExecuted"))
                           .when(F.col("side") == "Sell",
                                 (((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2) - ((F.col("at_end_plus60_best_bid_price") + F.col("at_end_plus60_best_ask_price")) / 2)) * F.col("fill_SharesExecuted"))
                           .otherwise(None))
        joined_df = joined_df.withColumn("mv60m", (((F.col("at_end_plus60_best_bid_price") + F.col("at_end_plus60_best_ask_price")) / 2) * F.col("fill_SharesExecuted")))

        joined_df = joined_df.withColumn("plarrival",
                           F.when(F.col("side") == "Buy",
                                  (((F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2 - as_of_df["fill_AveragePrice"]) * as_of_df["fill_SharesExecuted"]))
                           .when(F.col("side") == "Sell",
                                 ((as_of_df["fill_AveragePrice"] - (F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2) * as_of_df["fill_SharesExecuted"]))
                           .otherwise(None))

        joined_df = joined_df.withColumn("mvarrival",
                                         ((F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2) * as_of_df["fill_SharesExecuted"])

        joined_df = joined_df.withColumn("plvwap",
                                         F.when(F.col("side") == "Buy",
                                                (F.col("vwap") - as_of_df["fill_AveragePrice"]) * as_of_df["fill_SharesExecuted"])
                                         .when(F.col("side") == "Sell",
                                               (as_of_df["fill_AveragePrice"] - F.col("vwap")) * as_of_df["fill_SharesExecuted"])
                                         .otherwise(None))

        joined_df = joined_df.withColumn("mvvwap", as_of_df["fill_SharesExecuted"] * F.col("vwap"))

        joined_df = joined_df.withColumn("opportunitycostpl",
                           F.when(F.col("side") == "Buy",
                                  (F.col("SharesOrdered") - as_of_df["fill_SharesExecuted"]) * (as_of_df["fill_AveragePrice"] - F.col("close_price")))
                           .when(F.col("side") == "Sell",
                                 (F.col("SharesOrdered") - as_of_df["fill_SharesExecuted"]) * (F.col("close_price") - as_of_df["fill_AveragePrice"]))
                           .otherwise(None))

        joined_df = joined_df.withColumn("opportunitycostmv", (F.col("sharesordered") - as_of_df["fill_SharesExecuted"]) * as_of_df["fill_AveragePrice"])

        joined_df = joined_df.withColumn("currentmomentumpl",
                           F.when(F.col("side") == "Buy",
                                  (F.col("close_price") - (F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2) * F.col("SharesOrdered"))
                           .when(F.col("side") == "Sell",
                                 ((F.col("at_end_best_bid_price") + F.col("at_end_best_ask_price")) / 2 - F.col("close_price")) * F.col("SharesOrdered"))
                           .otherwise(None))

        joined_df = joined_df.withColumn("momentummv", F.col("close_price") * F.col("SharesOrdered"))

        joined_df = joined_df.withColumn("orderstartmomentum",
                           F.when(F.col("side") == "Buy",
                                  (F.col("close_price_prior") - (F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2) * F.col("SharesOrdered"))
                           .when(F.col("side") == "Sell",
                                 ((F.col("at_start_best_bid_price") + F.col("at_start_best_ask_price")) / 2 - F.col("close_price_prior")) * F.col("SharesOrdered"))
                           .otherwise(None))

        joined_df = joined_df.withColumn("desk_id", F.lit(""))
        joined_df = joined_df.withColumn("is_active", F.col("EndTime").isNull())

        joined_df = (joined_df.withColumnRenamed("inaccessible_pct", "inaccessible_liquidity_percent")
                             .withColumnRenamed("retail_pct", "retail_percent")
                             .withColumnRenamed("vwap", "interval_vwap")
                             .withColumnRenamed("at_end_plus1_best_bid_price", "bid_at_1m")
                             .withColumnRenamed("at_end_plus1_best_ask_price", "offer_at_1m")
                             .withColumnRenamed("at_end_plus5_best_bid_price", "bid_at_5m")
                             .withColumnRenamed("at_end_plus5_best_ask_price", "offer_at_5m")
                             .withColumnRenamed("at_end_plus60_best_bid_price", "bid_at_60m")
                             .withColumnRenamed("at_end_plus60_best_ask_price", "offer_at_60m")
                             .withColumnRenamed("fill_FirstFillTime", "FirstFillTime")
                             .withColumnRenamed("fill_SharesExecuted", "SharesExecuted")
                             .withColumnRenamed("fill_NumFills", "NumFills")
                             .withColumnRenamed("fill_AveragePrice", "AveragePrice")
                             .withColumnRenamed("fill_marketvalueordered", "marketvalueordered")
                             .withColumnRenamed("fill_MarketValueExecuted", "MarketValueExecuted")
                             .withColumnRenamed("at_start_best_bid_price", "Bid_at_Start")
                             .withColumnRenamed("at_start_best_ask_price", "Offer_at_Start")
                             .withColumnRenamed("at_end_best_bid_price", "Bid_at_End")
                             .withColumnRenamed("at_end_best_ask_price", "Offer_at_End")
                             .withColumnRenamed("cust_id", "CustomerID")
                     )

        joined_df = joined_df.select("fl_id", "rec_id", "orderid", "ordtype", as_of_df["cl_ordid"], "desk_id", "CustomerID", "traderid", as_of_df["symbol"], as_of_df["side"],
                         "sharesordered", as_of_df["limitprice"], "brokerid", "algorithm", "tactic", "starttime", "FirstFillTime",
                         "endtime", "SharesExecuted", "NumFills", "AveragePrice",
                         "marketvalueordered", "MarketValueExecuted", "Bid_at_Start", "Offer_at_Start",
                         "arrival_price", "Bid_at_End", "Offer_at_End", "interval_vwap", "interval_volume",
                         "limit_adjusted_vwap", "limit_adjusted_interval_volume", "close_price_prior",
                         "open_price", "close_price", "spread_capture", "momentum_category", "security_classification",
                         "averagedailyvolume", "inaccessible_liquidity_percent", "retail_percent", "pleod", "mveod",
                         "bid_at_1m", "offer_at_1m", "pl1m", "mv1m", "bid_at_5m", "offer_at_5m", "pl5m", "mv5m",
                         "bid_at_60m", "offer_at_60m", "pl60m", "mv60m", "plarrival", "mvarrival", "plvwap", "mvvwap",
                         "pllimitvwap", "mvlimitvwap", "opportunitycostpl", "opportunitycostmv", "currentmomentumpl",
                         "momentummv", "orderstartmomentum", "is_active", "transacttime", "trade_date", "target_compid",
                         "securityid", "securityid_source")

        joined_df = joined_df.dropDuplicates()
        hdfs_output_path = "hdfs://your_hdfs_namenode:8020/user/your_username/output_parquet"  # example
        joined_df.write.parquet(hdfs_output_path, mode="overwrite")

    def cleanup(self, spark: SparkSession):
        return spark.stop()
