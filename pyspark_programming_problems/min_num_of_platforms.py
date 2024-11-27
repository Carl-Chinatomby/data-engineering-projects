"""
Problem Statement (Goldman Sachs hard-level")
The problem is to calculate the minimum number of platforms required at a train station based on the given arrival_times and departure_times.

Problem Breakdown:
- We need to merge both arrival_time and departure_time into a unified dataset.
- We’ll use a window function to track how many platforms are required at each point in time.
- For each train arrival, we’ll add a platform (+1) and for each train departure, we’ll subtract a platform (-1).
- Finally, we will calculate the maximum number of platforms required at any point in time during the day.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    max as spark_max,
    sum as spark_sum,
)
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("minPlatforms") \
    .getOrCreate()

# Create Spark session
spark = SparkSession.builder.master("local[*]").appName("Train Platform Calculation").getOrCreate()

# Sample Data (Train Arrival and Departure times)
arrivals_data = [
    (1, '2024-11-17 08:00'),
    (2, '2024-11-17 08:05'),
    (3, '2024-11-17 08:05'),
    (4, '2024-11-17 08:10'),
    (5, '2024-11-17 08:10'),
    (6, '2024-11-17 12:15'),
    (7, '2024-11-17 12:20'),
    (8, '2024-11-17 12:25'),
    (9, '2024-11-17 15:00'),
    (10, '2024-11-17 15:00'),
    (11, '2024-11-17 15:00'),
    (12, '2024-11-17 15:06'),
    (13, '2024-11-17 20:00'),
    (14, '2024-11-17 20:10')
]

departures_data = [
    (1, '2024-11-17 08:15'),
    (2, '2024-11-17 08:10'),
    (3, '2024-11-17 08:20'),
    (4, '2024-11-17 08:25'),
    (5, '2024-11-17 08:20'),
    (6, '2024-11-17 13:00'),
    (7, '2024-11-17 12:25'),
    (8, '2024-11-17 12:30'),
    (9, '2024-11-17 15:05'),
    (10, '2024-11-17 15:10'),
    (11, '2024-11-17 15:15'),
    (12, '2024-11-17 15:15'),
    (13, '2024-11-17 20:15'),
    (14, '2024-11-17 20:15')
]

# Define schema for the data
arrival_schema = ['train_id', 'arrival_time']
departure_schema = ['train_id', 'departure_time']

arrivals_df = spark.createDataFrame(arrivals_data, arrival_schema)
departures_df = spark.createDataFrame(departures_data, departure_schema)

# Cast to datetime
arrivals_df = arrivals_df.withColumn('arrival_time', col('arrival_time').cast('timestamp'))
departures_df = departures_df.withColumn('departure_time', col('departure_time').cast('timestamp'))

# Add event type (arrival = 1, departure = -1)
arrivals_df = arrivals_df.withColumn('event_type', lit(1))
departures_df = departures_df.withColumn('event_type', lit(-1))

# Union both DataFrames into one, marking events as either arrival or departure
all_events_df = arrivals_df.select('train_id', 'arrival_time', 'event_type') \
    .withColumnRenamed('arrival_time', 'event_time') \
    .union(departures_df.select('train_id', 'departure_time', 'event_type') \
           .withColumnRenamed('departure_time', 'event_time'))

# Sort events by event_time (earliest first) to process arrivals and departures in correct order
all_events_df = all_events_df.orderBy('event_time')

# Step 2: Use a window function to calculate the running total of platforms needed at each time
# TODO: This does not account for overlapping times
window_spec = Window.orderBy('event_time')

# Calculate running sum of platforms needed
all_events_df = all_events_df.withColumn('platforms_needed', spark_sum('event_type').over(window_spec))

# Step 3: Find the maximum platforms_needed at any given time
max_platforms = all_events_df.agg(spark_max('platforms_needed')).take(1)[0][0]

# Output the result
print(f"The minimum number of platforms required: {max_platforms}")
