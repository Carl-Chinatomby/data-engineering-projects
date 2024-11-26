"""
Find the region producing the 5th highest revenue from the given data (csv file) in the last 28 days.

Input —
|region_name|revenue| date|
+ — — — — — -+ — — — -+ — — —- +
| region1| 100|2024–05–15|
| region2| 150|2024–05–15|
| region5| 200|2024–05–15|
| region4| 250|2024–05–15|
| region1| 110|2024–05–16|
| region2| 120|2024–05–16|
| region3| 130|2024–05–16|
| region5| 140|2024–05–16|
| region1| 105|2024–05–17|
| region2| 135|2024–05–17|
| region3| 145|2024–05–17|
| region4| 155|2024–05–17|
| region1| 115|2024–05–18|
| region5| 125|2024–05–18|
| region3| 135|2024–05–18|
| region4| 145|2024–05–18|
| region1| 120|2024–05–19|

Output —
|region_name|total_revenue|rank|
+ — — — — — -+ — — — — — — -+ — — +
| region2| 405| 5|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    desc,
    current_date,
    date_sub,
    rank,
    to_date,
)
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder \
    .appName("5th Highest Revenue Region") \
    .getOrCreate()

df = spark.read.csv('data/revenue.csv', header=True, inferSchema=True)
df = df.withColumn("newdate", to_date(col("date"), 'yyyy-MM-dd'))

# include only the last 28 days
df_filtered = df.filter(col("date") >= date_sub(current_date(), 28))

df_grouped = df_filtered.groupBy("region_name").agg(_sum("revenue").alias("total_revenue"))

window_spec = Window.orderBy(desc("total_revenue"))

df_ranked = df_grouped.withColumn("rank", rank().over(window_spec))

df_5th_highest = df_ranked.filter(col("rank") == 5)

df_5th_highest.show()
