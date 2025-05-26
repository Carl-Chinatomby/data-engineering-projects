"""
Problem Statement (Microsoft)"

You are tasked with finding the number of transactions that occurred for each
product. The output should include the product name along with the
corresponding number of transactions, and the records should be ordered
product_id in ascending order. You can ignore products without transactions.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max

spark = SparkSession.builder.appName("TransactionsPerProduct").getOrCreate()

inventory_data = [
    (1, 'strawberry', 'produce', 'lb', 3.28, 1.77, 13),
    (2, 'apple_fuji', 'produce', 'lb', 1.44, 0.43, 2),
    (3, 'orange', 'produce', 'lb', 1.02, 0.37, 2),
    (4, 'clementines', 'produce', 'lb', 1.19, 0.44, 44),
    (5, 'blood_orange', 'produce', 'lb', 3.86, 1.66, 19)
]
inventory_columns = ["product_id", "product_name", "product_type", "unit", "price_unit", "wholesale", "current_inventory"]

transaction_data = [
    (153, '2016-01-06 08:57:52', 1),
    (91, '2016-01-07 12:17:27', 1),
    (31, '2016-01-05 13:19:25', 1),
    (24, '2016-01-03 10:47:44', 3),
    (4, '2016-01-06 17:57:42', 3),
    (163, '2016-01-03 10:11:22', 3),
    (92, '2016-01-08 12:03:20', 2),
    (32, '2016-01-04 19:37:14', 4),
    (253, '2016-01-06 14:15:20', 5),
    (118, '2016-01-06 14:27:33', 5)
]
transaction_columns = ["transaction_id", "time", "product_id"]

inventory_df = spark.createDataFrame(inventory_data, inventory_columns)
transaction_df = spark.createDataFrame(transaction_data, transaction_columns)

joined_df = inventory_df.join(transaction_df, on="product_id", how="inner")
transaction_count_df = joined_df.groupBy("product_name", "product_id").agg(count("transaction_id").alias("transaction_count"))

result_df = transaction_count_df.select("product_name", "transaction_count").orderBy("product_id")
result_df.show()
spark.stop()