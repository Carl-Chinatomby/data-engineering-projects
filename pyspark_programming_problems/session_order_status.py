"""
Problem Overview (Walmart hard-level)
We have two datasets:

- Sessions Table: Contains records of when users started their sessions.
- Order Summary Table: Contains records of orders placed by users along with their values.

We want to:
- Find users who started a session and placed an order on the same day.
- Calculate the total number of orders and the total order value for those users.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as spark_sum,
)

spark = SparkSession.builder \
    .appName("SessionOrderAnalysis") \
    .getOrCreate()

# Sample data for sessions
sessions_data = [
    (1, 1, '2024-01-01 00:00:00'),
    (2, 2, '2024-01-02 00:00:00'),
    (3, 3, '2024-01-05 00:00:00'),
    (4, 3, '2024-01-05 00:00:00'),
    (5, 4, '2024-01-03 00:00:00'),
    (6, 4, '2024-01-03 00:00:00'),
    (7, 5, '2024-01-04 00:00:00'),
    (8, 5, '2024-01-04 00:00:00'),
    (9, 3, '2024-01-05 00:00:00'),
    (10, 5, '2024-01-04 00:00:00')
]
sessions_columns = ["session_id", "user_id", "session_date"]

# Sample data for orders
orders_data = [
    (1, 1, 152, '2024-01-01 00:00:00'),
    (2, 2, 485, '2024-01-02 00:00:00'),
    (3, 3, 398, '2024-01-05 00:00:00'),
    (4, 3, 320, '2024-01-05 00:00:00'),
    (5, 4, 156, '2024-01-03 00:00:00'),
    (6, 4, 121, '2024-01-03 00:00:00'),
    (7, 5, 238, '2024-01-04 00:00:00'),
    (8, 5, 70, '2024-01-04 00:00:00'),
    (9, 3, 152, '2024-01-05 00:00:00'),
    (10, 5, 171, '2024-01-04 00:00:00')
]
orders_columns = ["order_id", "user_id", "order_value", "order_date"]

# convert data into dataframes
sessions_df = spark.createDataFrame(sessions_data, sessions_columns)
orders_df = spark.createDataFrame(orders_data, orders_columns)

# convert columns into relevant date types ignore time
sessions_df = sessions_df.withColumn("session_date", to_date(col("session_date")))
orders_df = orders_df.withColumn("order_date", to_date(col("order_date")))

joined_df = sessions_df.alias("s").join(orders_df.alias("o"),
    (col("s.user_id") == col("o.user_id")) & (col("s.session_date") == col("o.order_date")),
    "inner"
)

# Group by user_id and session_date, and calculate the total number of orders and total value of orders
result_df = joined_df.groupBy('s.user_id', 's.session_date') \
    .agg(count('o.order_id').alias('total_orders'),
         spark_sum('o.order_value').alias('total_order_value'))

# Show the final result
result_df.show(truncate=False)
