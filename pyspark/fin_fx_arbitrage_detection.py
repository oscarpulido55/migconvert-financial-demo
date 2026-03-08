import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def detect_triangular_arbitrage():
    """
    Ingests high-frequency Foreign Exchange (FX) tick data.
    Simulates finding triangular arbitrage opportunities (e.g., USD->EUR->GBP->USD)
    using complex self-joins, floating temporal windowing, and precise cross-rate calculations.
    """
    spark = SparkSession.builder \
        .appName("FX_Arbitrage_Detection_Engine") \
        .config("spark.sql.broadcastTimeout", "3600") \
        .getOrCreate()

    # Base high-frequency ticks: schema = [tick_time, pair, bid, ask]
    ticks = spark.read.parquet("hdfs://fx_orderbook_ticks/")
    
    # Isolate major pairs for the triangle: EUR/USD, GBP/USD, EUR/GBP
    eur_usd = ticks.filter(F.col("pair") == "EUR/USD").withColumnRenamed("bid", "eur_usd_bid").withColumnRenamed("ask", "eur_usd_ask")
    gbp_usd = ticks.filter(F.col("pair") == "GBP/USD").withColumnRenamed("bid", "gbp_usd_bid").withColumnRenamed("ask", "gbp_usd_ask")
    eur_gbp = ticks.filter(F.col("pair") == "EUR/GBP").withColumnRenamed("bid", "eur_gbp_bid").withColumnRenamed("ask", "eur_gbp_ask")

    # To detect arbitrage, we need to view prices existing simultaneously.
    # We define a rolling lookback window to find the most recent quote for the other pairs
    window_spec = Window.partitionBy().orderBy(F.col("tick_time").cast("long")).rangeBetween(-2, 0) # 2 second buffer
    
    # We anchor the time to EUR/USD ticks, pulling the last known state of GBP/USD and EUR/GBP
    merged_ticks = eur_usd.alias("eu") \
        .join(gbp_usd.alias("gu"), 
             (F.col("gu.tick_time") <= F.col("eu.tick_time")) & 
             ((F.col("eu.tick_time").cast("long") - F.col("gu.tick_time").cast("long")) <= 2), 
             "left") \
        .join(eur_gbp.alias("eg"), 
             (F.col("eg.tick_time") <= F.col("eu.tick_time")) & 
             ((F.col("eu.tick_time").cast("long") - F.col("eg.tick_time").cast("long")) <= 2), 
             "left")

    # Due to the time-range join, we may have multiple permutations.
    # Take only the strictest, most recent tick combination
    dedup_window = Window.partitionBy("eu.tick_time").orderBy(F.col("gu.tick_time").desc(), F.col("eg.tick_time").desc())
    
    clean_ticks = merged_ticks.withColumn("rank", F.row_number().over(dedup_window)) \
                              .filter(F.col("rank") == 1).drop("rank")

    # Calculate Triangular Arbitrage Permutations
    # Case 1: Sell Dollars -> Buy Euros -> Buy Pounds -> Sell Pounds -> Receive Dollars
    # Maths: (1 / eur_usd_ask) * eur_gbp_bid * gbp_usd_bid
    
    # Case 2: Sell Dollars -> Buy Pounds -> Buy Euros -> Sell Euros -> Receive Dollars
    # Maths: (1 / gbp_usd_ask) * (1 / eur_gbp_ask) * eur_usd_bid
    
    arb_eval = clean_ticks.withColumn(
        "path_1_return", (F.lit(1.0) / F.col("eur_usd_ask")) * F.col("eur_gbp_bid") * F.col("gbp_usd_bid")
    ).withColumn(
        "path_2_return", (F.lit(1.0) / F.col("gbp_usd_ask")) * (F.lit(1.0) / F.col("eur_gbp_ask")) * F.col("eur_usd_bid")
    )

    # An arb exists when return > 1.0 (excluding transaction latency/fees for demo)
    detected_arbs = arb_eval.filter((F.col("path_1_return") > 1.0001) | (F.col("path_2_return") > 1.0001))
    
    flagged_arbs = detected_arbs.select(
        F.col("eu.tick_time").alias("detection_timestamp"),
        "eur_usd_ask", "eur_usd_bid",
        "gbp_usd_ask", "gbp_usd_bid",
        "eur_gbp_ask", "eur_gbp_bid",
        "path_1_return", "path_2_return"
    ).withColumn(
        "max_yield_bps", F.greatest(F.col("path_1_return"), F.col("path_2_return")) * 10000 - 10000
    )

    flagged_arbs.write.mode("append").parquet("hdfs://trading_analytics/arbs_detected/")

if __name__ == "__main__":
    detect_triangular_arbitrage()
