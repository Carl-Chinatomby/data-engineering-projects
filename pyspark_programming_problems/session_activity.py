"""
Problem Statement (Amazon Hard-level)
We are given a table called customer_state_log containing the following columns:

- cust_id: The ID of the customer.
- state: The state of the session, where 1 indicates the session is active and 0 indicates the session has ended.
- timestamp: The timestamp when the state change occurred.

Our task is to calculate how many hours each user was active during the day based on the state transitions.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lag,
    unix_timestamp,
    sum as spark_sum,
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("CustomerSessionHours") \
    .getOrCreate()


# Sample data (as given in the problem)
data = [
    ('c001', 1, '07:00:00'),
    ('c001', 0, '09:30:00'),
    ('c001', 1, '12:00:00'),
    ('c001', 0, '14:30:00'),
    ('c002', 1, '08:00:00'),
    ('c002', 0, '09:30:00'),
    ('c002', 1, '11:00:00'),
    ('c002', 0, '12:30:00'),
    ('c002', 1, '15:00:00'),
    ('c002', 0, '16:30:00'),
    ('c003', 1, '09:00:00'),
    ('c003', 0, '10:30:00'),
    ('c004', 1, '10:00:00'),
    ('c004', 0, '10:30:00'),
    ('c004', 1, '14:00:00'),
    ('c004', 0, '15:30:00'),
    ('c005', 1, '10:00:00'),
    ('c005', 0, '14:30:00'),
    ('c005', 1, '15:30:00'),
    ('c005', 0, '18:30:00')
]
columns = ["cust_id", "state", "timestamp"]

df = spark.createDataFrame(data, columns)

# Convert 'timestamp' to a proper timestamp type (using 'HH:MM:SS' format)
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Define a window specification to partition by cust_id and order by timestamp
window_spec = Window.partitionBy("cust_id").orderBy("timestamp")

# 1. Calculate the previous timestamp for each row using LAG() function
df_with_lag = df.withColumn("prev_timestamp", lag("timestamp").over(window_spec))

# 2. Filter out rows where state is 0 (session ends)
session_ends = df_with_lag.filter(df_with_lag.state == 0)

# 3. Calculate session duration in minutes (difference between the session end time and start time)
session_duration = session_ends.withColumn(
    "session_duration_minutes",
    (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60
)

# 4. Group by cust_id and calculate the total active time in hours
result = session_duration.groupBy("cust_id").agg(
    spark_sum("session_duration_minutes").alias("total_active_minutes")
)

# Convert total active minutes to hours
result = result.withColumn("total_active_hours", col("total_active_minutes") / 60)

# Show the final result
result.show()

spark.stop()
