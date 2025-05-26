"""
Problem Statement:

We often need to generate complex business metrics that require:
- Multiple levels of aggregations
- Time-based calculations
- Custom business logic
- Memory-efficient processing of large datasets

This implements a multi-stage aggregation pipeline that:
- Uses memory-efficient operations
- Implements checkpoint mechanisms
- Handles skewed data
- Provides business-ready metrics
"""
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BASE_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/../../"


class SalesAggregationPipeline:
    def __init__(self, spark):
        self.spark = spark
        self.checkpoint_dir = f"{BASE_PATH}checkpoint/sales_agg"
        spark.sparkContext.setCheckpointDir(self.checkpoint_dir)

    def read_data(self, input_path):
        return self.spark.read.parquet(input_path)

    def calculate_daily_metrics(self, df):
        return df.groupBy("date", "country", "category") \
            .agg(
                F.count("transaction_id").alias("total_transactions"),
                F.sum("payment_amount").alias("total_revenue"),
                F.avg("payment_amount").alias("avg_order_value"),
                F.sum("transaction_fee").alias("total_fees"),
                F.sum("tax").alias("total_tax"),
                F.countDistinct("customer_id").alias("unique_customers")
            )

    def calculate_moving_averages(self, daily_metrics):
        # Convert date string to date type if it's not already
        daily_metrics_with_date = daily_metrics.withColumn(
            "date_ts",
            F.to_date(F.col("date"))
        )

        # Use rowsBetween instead of rangeBetween for a 7-day window
        window_7d = Window.partitionBy("country", "category") \
            .orderBy("date_ts") \
            .rowsBetween(-6, 0)

        return daily_metrics_with_date \
            .withColumn("revenue_7d_avg",
                F.avg("total_revenue").over(window_7d)) \
            .withColumn("transactions_7d_avg",
                F.avg("total_transactions").over(window_7d))

    def calculate_category_performance(self, df):
        # Define an empty window spec for global aggregation
        window_spec = Window.partitionBy()

        return df.groupBy("category") \
            .agg(
                F.sum("payment_amount").alias("category_revenue"),
                F.count("transaction_id").alias("category_transactions")
            ) \
            .withColumn("revenue_percentage",
                F.col("category_revenue") / F.sum("category_revenue").over(window_spec))

    def identify_top_products(self, df):
        return df.groupBy("product_id", "name", "category") \
            .agg(
                F.sum("payment_amount").alias("product_revenue"),
                F.count("transaction_id").alias("sales_count")
            ) \
            .withColumn("rank", F.dense_rank().over(
                Window.partitionBy("category")
                .orderBy(F.desc("product_revenue"))
            )) \
            .filter(F.col("rank") <= 10)

    def calculate_customer_segments(self, df):
        customer_purchases = df.groupBy("customer_id") \
            .agg(
                F.sum("payment_amount").alias("total_spent"),
                F.count("transaction_id").alias("purchase_count"),
                F.avg("payment_amount").alias("avg_purchase")
            )

        # Calculate percentiles first
        window_spec = Window.partitionBy()
        with_percentiles = customer_purchases \
            .withColumn("p90", F.expr("percentile_approx(total_spent, 0.9)").over(window_spec)) \
            .withColumn("p70", F.expr("percentile_approx(total_spent, 0.7)").over(window_spec)) \
            .withColumn("p40", F.expr("percentile_approx(total_spent, 0.4)").over(window_spec))

        # Then define segments using the pre-calculated percentiles
        return with_percentiles \
            .withColumn("segment",
                F.when(F.col("total_spent") > F.col("p90"), "Premium")
                .when(F.col("total_spent") > F.col("p70"), "High")
                .when(F.col("total_spent") > F.col("p40"), "Medium")
                .otherwise("Standard")) \
            .drop("p90", "p70", "p40")

    def run_pipeline(self, input_path, output_path):
        try:
            # Read data
            df = self.read_data(input_path)
            df.cache()

            # Calculate all metrics
            daily_metrics = self.calculate_daily_metrics(df)
            daily_metrics.checkpoint()  # Checkpoint for memory optimization

            moving_averages = self.calculate_moving_averages(daily_metrics)
            category_performance = self.calculate_category_performance(df)
            top_products = self.identify_top_products(df)
            customer_segments = self.calculate_customer_segments(df)

            # Save results
            moving_averages.write \
                .mode("overwrite") \
                .partitionBy("country") \
                .parquet(f"{output_path}/daily_metrics")

            category_performance.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/category_performance")

            top_products.write \
                .mode("overwrite") \
                .partitionBy("category") \
                .parquet(f"{output_path}/top_products")

            customer_segments.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/customer_segments")

            # Uncache data
            df.unpersist()

            return {
                "daily_metrics": moving_averages,
                "category_performance": category_performance,
                "top_products": top_products,
                "customer_segments": customer_segments
            }

        except Exception as e:
            print(f"Error in aggregation pipeline: {str(e)}")
            raise

def main():
    spark = SparkSession.builder \
        .appName("Sales Aggregation Pipeline") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()

    pipeline = SalesAggregationPipeline(spark)
    results = pipeline.run_pipeline(
        f"{BASE_PATH}output/flattened_sales",
        f"{BASE_PATH}output/aggregated_metrics"
    )

    # Print summary statistics
    for metric_name, df in results.items():
        print(f"\nSummary for {metric_name}:")
        df.show(5)

if __name__ == "__main__":
    main()