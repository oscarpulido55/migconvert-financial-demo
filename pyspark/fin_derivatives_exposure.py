import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

def calculate_portfolio_exposure():
    """
    Complex pySpark script demonstrating aggregation of Options Greeks, 
    synthetic underlying creation, and nested Value-at-Risk (VaR) estimations
    for a complex institutional derivatives portfolio.
    """
    spark = SparkSession.builder \
        .appName("Derivatives_Exposure_Aggregator") \
        .getOrCreate()
        
    execution_date = "2024-05-15"

    portfolios_df = spark.table("fin_core.institutional_portfolios")
    options_positions_df = spark.table("fin_core.positions_options").filter(F.col("pos_date") == execution_date)
    futures_positions_df = spark.table("fin_core.positions_futures").filter(F.col("pos_date") == execution_date)
    yield_curve_df = spark.table("fin_core.market_yield_curve").filter(F.col("curve_date") == execution_date)

    # 1. Normalize Futures to Delta Equivalents
    # Futures act essentially as 1 Delta per contract lot size
    futures_normalized = futures_positions_df.withColumn(
        "delta_usd", F.col("contract_qty") * F.col("lot_size") * F.col("settlement_price")
    ).withColumn("gamma_usd", F.lit(0.0)) \
     .withColumn("vega_usd", F.lit(0.0)) \
     .withColumn("theta_usd", F.lit(0.0))

    future_exposures = futures_normalized.groupBy("portfolio_id", "underlying_asset").agg(
        F.sum("delta_usd").alias("total_delta_usd"),
        F.sum("gamma_usd").alias("total_gamma_usd"),
        F.sum("vega_usd").alias("total_vega_usd"),
        F.sum("theta_usd").alias("total_theta_usd")
    )

    # 2. Normalize Options Greeks
    # Options have complex Greeks which form our primary non-linear risk footprint
    options_normalized = options_positions_df.withColumn(
        "position_value", F.col("contract_qty") * F.col("multiplier") * F.col("mark_price")
    ).withColumn(
        "delta_usd", F.col("contract_qty") * F.col("multiplier") * F.col("delta") * F.col("underlying_price")
    ).withColumn(
        "gamma_usd", 0.5 * F.col("contract_qty") * F.col("multiplier") * F.col("gamma") * F.pow(F.col("underlying_price"), 2) / 100
    ).withColumn(
        "vega_usd", F.col("contract_qty") * F.col("multiplier") * F.col("vega")
    ).withColumn(
        "theta_usd", F.col("contract_qty") * F.col("multiplier") * F.col("theta")
    )
    
    options_exposures = options_normalized.groupBy("portfolio_id", "underlying_asset").agg(
        F.sum("delta_usd").alias("total_delta_usd"),
        F.sum("gamma_usd").alias("total_gamma_usd"),
        F.sum("vega_usd").alias("total_vega_usd"),
        F.sum("theta_usd").alias("total_theta_usd")
    )

    # 3. Combine risk factors by Underlying Asset via Full Outer Join (since portfolios can have options without futures and vice versa)
    combined_exposure = options_exposures.alias("o").join(
        future_exposures.alias("f"), 
        (F.col("o.portfolio_id") == F.col("f.portfolio_id")) & (F.col("o.underlying_asset") == F.col("f.underlying_asset")),
        "full_outer"
    )

    consolidated_risk = combined_exposure.select(
        F.coalesce("o.portfolio_id", "f.portfolio_id").alias("portfolio_id"),
        F.coalesce("o.underlying_asset", "f.underlying_asset").alias("underlying_asset"),
        (F.coalesce("o.total_delta_usd", F.lit(0.0)) + F.coalesce("f.total_delta_usd", F.lit(0.0))).alias("net_delta_usd"),
        (F.coalesce("o.total_gamma_usd", F.lit(0.0)) + F.coalesce("f.total_gamma_usd", F.lit(0.0))).alias("net_gamma_usd"),
        (F.coalesce("o.total_vega_usd", F.lit(0.0)) + F.coalesce("f.total_vega_usd", F.lit(0.0))).alias("net_vega_usd"),
        (F.coalesce("o.total_theta_usd", F.lit(0.0)) + F.coalesce("f.total_theta_usd", F.lit(0.0))).alias("net_theta_usd")
    )

    # 4. Parametric VaR Construction
    # Fetch historical volatility per asset 
    asset_volatility = spark.table("fin_core.market_volatility_surface").filter(F.col("surface_date") == execution_date)

    # Approximate 1-Day 99% VaR (2.33 standard deviations) focusing on Delta-Gamma risk contribution
    risk_framework = consolidated_risk.alias("cr").join(asset_volatility.alias("vol"), "underlying_asset", "left")

    var_calc = risk_framework.withColumn(
        "var_99_1d_delta", F.abs(F.col("net_delta_usd") * F.col("vol.asset_daily_volatility") * 2.33)
    ).withColumn(
        "var_99_1d_gamma", 0.5 * F.abs(F.col("net_gamma_usd") * F.pow(F.col("vol.asset_daily_volatility") * 2.33, 2))
    )

    # 5. Portfolio Level Aggregations using nested map structures
    portfolio_summary = var_calc.groupBy("portfolio_id").agg(
        F.sum("net_delta_usd").alias("portfolio_net_delta"),
        F.sum("net_vega_usd").alias("portfolio_net_vega"),
        # Square Root of Sum of Squares (Assuming zero correlation for simplicity in this demo)
        F.sqrt(F.sum(F.pow(F.col("var_99_1d_delta") + F.col("var_99_1d_gamma"), 2))).alias("portfolio_uncorrelated_var")
    )

    # Output to target datamart
    portfolio_summary.write.mode("overwrite").saveAsTable("fin_mart.derivatives_exposure_metrics")
    print("Derivatives portfolio processing complete")

if __name__ == "__main__":
    calculate_portfolio_exposure()
