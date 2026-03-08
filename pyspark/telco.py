from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, datediff, avg, count

def process_telco_data_advanced(customer_path, usage_path, output_path):

    spark = SparkSession.builder.appName("AdvancedTelcoProcessing").getOrCreate()

    try:
        # Read data
        customer_df = spark.read.csv(customer_path, header=True, inferSchema=True)
        usage_df = spark.read.csv(usage_path, header=True, inferSchema=True)

        # Clean up date columns
        customer_df = customer_df.withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd"))
        usage_df = usage_df.withColumn("usage_date", to_date(col("usage_date"), "yyyy-MM-dd"))

        # Join customer and usage data
        joined_df = customer_df.join(usage_df, "customer_id", "inner")

        # More complex filters
        filtered_df = joined_df.filter(
            (col("plan_type") == "premium") &
            (col("monthly_charges") > 50) &
            (col("data_usage_gb") > 5) &
            (col("usage_date") >= "2023-01-01")
        )

        # Calculate contract length
        filtered_df = filtered_df.withColumn(
            "contract_length",
            datediff(col("end_date"), col("start_date"))
        )

        # Aggregate data: average usage per customer and plan type, and number of events.
        aggregated_df = filtered_df.groupBy("customer_id", "plan_type").agg(
            avg("data_usage_gb").alias("avg_data_usage"),
            count("*").alias("event_count"),
            avg("contract_length").alias("avg_contract_length")
        )

        # Filter the aggregated data again
        final_df = aggregated_df.filter(col("avg_data_usage") > 7)

        # Save the result
        final_df.write.mode("overwrite").parquet(output_path)

        print(f"Advanced telco data processed and saved to: {output_path}")

    except Exception as e:
        print(f"Error processing telco data: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    customer_file = "path/to/customer_data.csv"
    usage_file = "path/to/usage_data.csv"
    output_directory = "path/to/advanced_processed_telco_data"
    process_telco_data_advanced(customer_file, usage_file, output_directory)