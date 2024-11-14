"""
Calculate the utilization rate of each aircraft for every day. The utilization rate is defined as the total flight hours divided by the total hours available for each aircraft.

Given Input —
Flights —
|flight_id|aircraft_id| departure_time| arrival_time|
+ — — — — -+ — — — — — -+ — — — — — — — — — -+ — — — — — — — — — -+
| 1| A320|2022–06–08 10:00:00|2022–06–08 12:00:00|
| 2| A320|2022–06–09 15:00:00|2022–06–09 17:00:00|
| 3| 777|2022–06–26 08:00:00|2022–06–26 12:00:00|
| 4| 777|2022–07–01 20:00:00|2022–07–01 22:00:00|
| 5| A380|2022–07–05 11:00:00|2022–07–05 15:00:00|

Aircrafts —
|aircraft_id|availability_hours_per_day|
+ — — — — — -+ — — — — — — — — — — — — — +
| A320| 10|
| 777| 12|
| A380| 8|

Output —
| date|aircraft_id|utilization_rate|
+ — — — — — + — — — — — -+ — — — — — — — — +
|2022–07–05| A380| 0.5|
|2022–06–08| A320| 0.2|
|2022–06–09| A320| 0.2|
|2022–07–01| 777| 0.17|
|2022–06–26| 777| 0.33|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    date_format,
    sum as spark_sum,
    round as spark_round,
)

spark = SparkSession.builder \
    .appName("AircraftUtilization") \
    .getOrCreate()

flights_data = [
    (1, "A320", "2022-06-08 10:00:00", "2022-06-08 12:00:00"),
    (2, "A320", "2022-06-09 15:00:00", "2022-06-09 17:00:00"),
    (3, "777", "2022-06-26 08:00:00", "2022-06-26 12:00:00"),
    (4, "777", "2022-07-01 20:00:00", "2022-07-01 22:00:00"),
    (5, "A380", "2022-07-05 11:00:00", "2022-07-05 15:00:00")
]

aircrafts_data = [
    ("A320", 10),
    ("777", 12),
    ("A380", 8)
]

flights_columns = ["flight_id", "aircraft_id", "departure_time", "arrival_time"]
aircrafts_columns = ["aircraft_id", "availability_hours_per_day"]

flights_df = spark.createDataFrame(flights_data, flights_columns)
aircrafts_df = spark.createDataFrame(aircrafts_data, aircrafts_columns)

# Convert departure_time and arrival_time to timestamp
flights_df = flights_df.withColumn("departure_time", to_timestamp("departure_time")) \
    .withColumn("arrival_time", to_timestamp("arrival_time"))

# Calculate the duration of each flight
flights_df = flights_df.withColumn("flight_duration_hours",
    (col("arrival_time").cast("long") - col("departure_time").cast("long")) / 3600)

flights_df = flights_df.withColumn("date", date_format(col("departure_time"), "yyyy-MM-dd"))
total_flight_duration_df = flights_df.groupBy("date", "aircraft_id") \
    .agg(spark_sum("flight_duration_hours").alias("total_flight_duration_hours"))

utilization_df = total_flight_duration_df.join(aircrafts_df, on="aircraft_id")

utilization_df = utilization_df.withColumn("utilization_rate",
                                           spark_round(col("total_flight_duration_hours") / col("availability_hours_per_day"), 2))

utilization_df = utilization_df.select("date", "aircraft_id", "utilization_rate")

utilization_df.show()

spark.stop()

