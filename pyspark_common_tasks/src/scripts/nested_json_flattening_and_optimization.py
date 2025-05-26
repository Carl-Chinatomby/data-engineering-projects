"""
Problem Statement:

Working with nested JSON in production is challenging due to:
- Complex query performance on nested structures
- Difficulty in applying transformations
- Storage inefficiency
- Complicated maintenance

This implements a smart flattening strategy that:
- Preserves data lineage
- Optimizes storage
- Improves query performance
- Handles null values gracefully
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, concat_ws


BASE_PATH = os.path.dirname(os.path.realpath(__file__))


def create_spark_session():
    return SparkSession.builder \
        .appName("JSON Flattener") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()

def flatten_nested_json(spark, input_path, output_path):
    df = spark.read.json(input_path)

    flattened_df = df \
        .select(
            "transaction_id",
            "date",
            col("product.id").alias("product_id"),
            "product.name",
            "product.category",
            "product.specifications.weight",
            concat_ws("x",
                col("product.specifications.dimensions.length"),
                col("product.specifications.dimensions.width"),
                col("product.specifications.dimensions.height")
            ).alias("dimensions"),
            col("customer.id").alias("customer_id"),
            "customer.location.country",
            "customer.location.city",
            col("payment.method").alias("payment_method"),
            col("payment.amount").alias("payment_amount"),
            "payment.currency",
            "payment.status",
            "payment.details.transaction_fee",
            "payment.details.tax",
            col("shipping.method").alias("shipping_method"),
            col("shipping.cost").alias("shipping_cost"),
            "shipping.estimated_days"
        )

    # Optimize storage
    flattened_df = flattened_df.repartition(200)

    flattened_df.write \
        .mode("overwrite") \
        .partitionBy("country", "category") \
        .parquet(output_path)

    return flattened_df

def main():
    spark = create_spark_session()

    # Add logging
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(__name__)

    try:
        logger.info("Starting JSON flattening process")

        flattened_df = flatten_nested_json(
            spark,
            f"{BASE_PATH}/../../data/sales_data.json",
            f"{BASE_PATH}/../../output/flattened_sales"
        )

        # Validate output
        logger.info(f"Records processed: {flattened_df.count()}")
        logger.info("JSON flattening completed successfully")

    except Exception as e:
        logger.error(f"Error in flattening process: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()