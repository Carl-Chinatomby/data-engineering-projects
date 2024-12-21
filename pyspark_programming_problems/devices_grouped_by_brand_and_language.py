"""
Find the number of Apple product users (MacBook-Pro, iPhone 5s, iPad-air) and
the total number of users with any device, grouped by language. Output the
language along with the total number of Apple users and users with any device.
Order the results by the number of total users in descending order.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    when,
    coalesce,
    lit,
)

spark = SparkSession.builder \
    .appName("AppleUsers") \
    .getOrCreate()


# Sample data for playbook_users
users_data = [
    (1, "2024-01-01 08:00:00", 101, "English", "2024-01-05 10:00:00", "Active"),
    (2, "2024-01-02 09:00:00", 102, "Spanish", "2024-01-06 11:00:00", "Inactive"),
    (3, "2024-01-03 10:00:00", 103, "French", "2024-01-07 12:00:00", "Active"),
    (4, "2024-01-04 11:00:00", 104, "English", "2024-01-08 13:00:00", "Active"),
    (5, "2024-01-05 12:00:00", 105, "Spanish", "2024-01-09 14:00:00", "Inactive")
]

# Sample data for playbook_events
events_data = [
    (1, "2024-01-05 14:00:00", "Click", "Login", "USA", "MacBook-Pro"),
    (2, "2024-01-06 15:00:00", "View", "Dashboard", "Spain", "iPhone 5s"),
    (3, "2024-01-07 16:00:00", "Click", "Logout", "France", "iPad-air"),
    (4, "2024-01-08 17:00:00", "Purchase", "Subscription", "USA", "Windows-Laptop"),
    (5, "2024-01-09 18:00:00", "Click", "Login", "Spain", "Android-Phone")
]

# Define schema for users and events data
users_columns = ["user_id", "created_at", "company_id", "language", "activated_at", "state"]
events_columns = ["user_id", "occurred_at", "event_type", "event_name", "location", "device"]

users_df = spark.createDataFrame(users_data, users_columns)
events_df = spark.createDataFrame(events_data, events_columns)

# Define Apple devices
apple_devices = ["MacBook-Pro", "iPhone 5s", "iPad-air"]

# Apple Users CTE: Filter users with Apple devices and count distinct users per language
apple_device_events_df = events_df.filter(col("device").isin(apple_devices))
apple_users_df = apple_device_events_df.join(users_df, "user_id", "inner") \
    .groupBy("language") \
    .agg(countDistinct("user_id").alias("apple_users"))

# Total Users CTE: Count distinct users per language for any device
total_users_df = events_df.join(users_df, "user_id", "inner") \
    .groupBy('language') \
    .agg(countDistinct('user_id').alias('total_users'))

# Join Apple users and Total users, and handle nulls with COALESCE
final_df = apple_users_df.join(total_users_df, 'language', 'outer') \
    .select('language',
        coalesce('apple_users', lit(0)).alias('apple_users'),
        coalesce('total_users', lit(0)).alias('total_users')
    )
final_df = final_df.withColumn('perentage', col('apple_users')/(col('total_users')))

final_df.show()
spark.stop()
