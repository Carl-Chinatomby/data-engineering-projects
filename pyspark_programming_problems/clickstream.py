'''
Given a clickstream of user activity data , find the relevant user session for each click event.

click_time | user_id
2018–01–01 11:00:00 | u1
2018–01–01 12:00:00 | u1
2018–01–01 13:00:00 | u1
2018–01–01 13:00:00 | u1
2018–01–01 14:00:00 | u1
2018–01–01 15:00:00 | u1
2018–01–01 11:00:00 | u2
2018–01–02 11:00:00 | u2

session definition:
1. session expires after inactivity of 30mins, because of inactivity no clickstream will be generated
2. session remain active for total of 2 hours
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    unix_timestamp,
    lag,
    when,
    lit,
    concat,
    sum,
    monotonically_increasing_id,
)
from pyspark.sql.window import Window


# Create a SparkSession
spark = SparkSession.builder \
    .appName('ClickStreamSession') \
    .getOrCreate()

# Define the schema for the clickstream data
schema = 'click_time STRING, user_id STRING'

# Sample clickstream data
data = [
    ('2018-01-01 11:00:00', 'u1'),
    ('2018-01-01 12:00:00', 'u1'),
    ('2018-01-01 13:00:00', 'u1'),
    ('2018-01-01 13:00:00', 'u1'),
    ('2018-01-01 14:00:00', 'u1'),
    ('2018-01-01 15:00:00', 'u1'),
    ('2018-01-01 11:00:00', 'u2'),
    ('2018-01-02 11:00:00', 'u2'),
]

# Create a DataFrame  from the given sample data
clickstream_df = spark.createDataFrame(data, schema=schema)

# Convert click_time to Unix timestamp for easier calculations
clickstream_df = clickstream_df.withColumn('click_timestamp', unix_timestamp('click_time'))

session_window = Window.partitionBy('user_id').orderBy('click_timestamp')

# Getting the previous row using lag
clickstream_df = clickstream_df.withColumn('prev_click_timestamp', lag('click_timestamp', 1).over(session_window))

# #  Difference between click time and dividing that with 60
clickstream_df= clickstream_df.withColumn('timestamp_diff', (col('click_timestamp') - col('prev_click_timestamp'))/60)

# # Updating null with 0
clickstream_df = clickstream_df.withColumn('timestamp_diff', when(col('timestamp_diff').isNull(), 0).otherwise(col('timestamp_diff')))

# # Check for new sessions
clickstream_df = clickstream_df.withColumn('session_new', when(col('timestamp_diff') > 30, 1).otherwise(0))

# # New session names
clickstream_df = clickstream_df.withColumn('session_new_name', concat(col('user_id'), lit('--S'), sum(col("session_new")).over(session_window)))
clickstream_df.show()
