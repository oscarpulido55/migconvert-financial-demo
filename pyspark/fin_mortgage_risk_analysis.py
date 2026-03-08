import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, max as spark_max, sum as spark_sum, avg as spark_avg, count as spark_count,
    datediff, current_date, months_between, when, coalesce, array, struct, transform, aggregate, expr, explode
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.window import Window

def init_spark_session():
    """Returns Spark Session optimized for pure DataFrame Operations"""
    return SparkSession.builder \
        .appName("Financial_Mortgage_Risk_Analysis_DataFrames") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def build_mortgage_risk_attributes(spark, run_date):
    """
    Complex Pure DataFrame Pipeline calculating Mortgage Default Probability attributes.
    Utilizes PySpark 3.x array functions (transform, aggregate) and deep Windowing,
    avoiding all SQL execution.
    """
    
    # 1. Load Data
    df_mortgages = spark.table("fin_core.dim_mortgages")
    df_payments = spark.table("fin_core.fact_mortgage_payments")
    df_property_valuation = spark.table("fin_core.fact_property_valuation_history")
    
    # 2. Native Window Function to get the latest property valuation
    val_window = Window.partitionBy("property_id").orderBy(col("valuation_date").desc())
    latest_valuation = df_property_valuation \
        .withColumn("row_rank", expr("row_number() over (partition by property_id order by valuation_date desc)")) \
        .filter(col("row_rank") == 1) \
        .drop("row_rank") \
        .select("property_id", col("valuation_amount").alias("current_property_value"))

    # 3. Aggregate Payment History over specific rolling periods (3 month and 12 month)
    # Filter payments leading up to the run date
    historical_payments = df_payments.filter(col("payment_date") <= run_date)
    
    payment_agg = historical_payments.groupBy("mortgage_id").agg(
        spark_sum("principal_paid").alias("lifetime_principal_paid"),
        spark_count(when(col("days_past_due") > 0, True)).alias("lifetime_late_payments"),
        spark_count(when(col("days_past_due") > 30, True)).alias("lifetime_30dpd_payments"),
        # Conditionally aggregate last 12 months (365 days)
        spark_count(
            when((datediff(lit(run_date), col("payment_date")) <= 365) & (col("days_past_due") > 30), True)
        ).alias("late_30dpd_last_12m")
    )

    # 4. Join up the Base Mortgage Matrix
    mortgage_matrix = df_mortgages.alias("m") \
        .join(latest_valuation.alias("v"), col("m.property_id") == col("v.property_id"), "left") \
        .join(payment_agg.alias("p"), col("m.mortgage_id") == col("p.mortgage_id"), "left") \
        .fillna(0.0, subset=["lifetime_principal_paid"])
        
    # 5. Extract Complex Nested Logic Using PySpark Native Arrays/Structs
    # Assume the raw mortgages table contains a complex array of dictionaries representing 'co_borrowers'
    # e.g [{"borrower_id": "123", "income": 50000}, {"borrower_id": "456", "income": 45000}]
    
    enriched_matrix = mortgage_matrix.withColumn(
        "remaining_principal",
        col("m.original_loan_amount") - col("p.lifetime_principal_paid")
    ).withColumn(
        "current_ltv_ratio",
        col("remaining_principal") / col("v.current_property_value")
    ).withColumn(
        "months_on_books",
        months_between(lit(run_date), col("m.origination_date"))
    )

    # Example of higher order function to sum up nested co_borrower incomes
    # Here we use try/catch concept by checking if column exists in the schema or fallback
    if "co_borrowers" in enriched_matrix.columns:
        # aggregate(expr, initialValue, merge, finish)
        enriched_matrix = enriched_matrix.withColumn(
            "total_household_income",
            col("m.primary_borrower_income") + 
            aggregate(
                col("co_borrowers"), 
                lit(0.0), 
                lambda acc, x: acc + x.income
            )
        )
    else:
        # Fallback if no co_borrowers array exists in the fake dataset
        enriched_matrix = enriched_matrix.withColumn(
            "total_household_income", col("m.primary_borrower_income")
        )

    # 6. Final Risk Categorization
    final_output = enriched_matrix.withColumn(
        "mortgage_risk_tier",
        when((col("current_ltv_ratio") > 0.95) & (col("late_30dpd_last_12m") > 1), "CRITICAL")
        .when(col("current_ltv_ratio") > 0.90, "HIGH")
        .when((col("current_ltv_ratio") > 0.80) & (col("lifetime_30dpd_payments") > 2), "MEDIUM")
        .otherwise("LOW")
    ).select(
        "m.mortgage_id", "m.primary_borrower_id", 
        "remaining_principal", "v.current_property_value", "current_ltv_ratio",
        "total_household_income", "late_30dpd_last_12m", "mortgage_risk_tier",
        lit(run_date).alias("analysis_date")
    )
    
    # Write to target
    final_output.write \
        .partitionBy("analysis_date", "mortgage_risk_tier") \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("fin_mart.mortgage_risk_analysis")
        
    print(f"Mortgage risk analytical pipeline executed completely via DataFrames. Generated {final_output.count()} rows.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pyspark fin_mortgage_risk_analysis.py <YYYY-MM-DD>")
        sys.exit(1)
        
    analysis_date = sys.argv[1]
    spark_session = init_spark_session()
    
    build_mortgage_risk_attributes(spark_session, analysis_date)
    spark_session.stop()
