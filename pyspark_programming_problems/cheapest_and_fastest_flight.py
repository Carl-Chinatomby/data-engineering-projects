"""
Find the cheapest and fastest airline for each travel date and mark the flights accordingly,
‘Yes’ for the flights which are cheapest or fastest and with ‘No’ for rest of the airline

Given Input —
airline| date| travel_duration| price
+ — — — -+ — — — -+ — — — — — — -+ — — — -+
indigo| 21/03/2024| 1:10| 5000
airindia| 21/03/2024| 2:00| 3500
delta| 21/03/2024| 2:00| 2000
indigo| 22/03/2024| 1:10| 5000
delta| 22/03/2024| 2:15| 1500
vistara| 22/03/2024| 1:00| 6000

Output —
+ — — — — + — — — — — + — — — — — — — -+ — — -+ — — — — + — — — — +
| airline| date|travel_duration|price|cheapest|fastest|
+ — — — — + — — — — — + — — — — — — — -+ — — -+ — — — — + — — — — +
|indigo|21/03/2024| 1:10| 5000| No| Yes|
|airindia|21/03/2024| 2:00| 3500| No| No|
| delta|21/03/2024| 2:00| 2000| Yes| No|
| indigo|22/03/2024| 1:10| 5000| No| No|
| delta|22/03/2024| 2:15| 1500| Yes| No|
| vistara|22/03/2024| 1:00| 6000| No| Yes|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    rank,
    when,
    min as spark_min,
    udf
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType


spark = SparkSession.builder \
    .appName("CheapestFastestFlights") \
    .getOrCreate()

data = [
    ("indigo", "21/03/2024", "1:10", 5000),
    ("airindia", "21/03/2024", "2:00", 3500),
    ("delta", "21/03/2024", "2:00", 2000),
    ("indigo", "22/03/2024", "1:10", 5000),
    ("delta", "22/03/2024", "2:15", 1500),
    ("vistara", "22/03/2024", "1:00", 6000)
]

columns = ["airline", "date", "travel_duration", "price"]
df = spark.createDataFrame(data, columns)

# Function to convert travel duration to minutes
def duration_to_minutes(duration):
    hours, minutes = map(int, duration.split(':'))
    return hours * 60 + minutes

duration_to_minutes_udf = udf(duration_to_minutes, IntegerType())

df = df.withColumn("duration_minutes", duration_to_minutes_udf(col("travel_duration")))

price_window = Window.partitionBy("date").orderBy("price")
duration_window = Window.partitionBy("date").orderBy("duration_minutes")

df = df.withColumn("price_rank", rank().over(price_window))
df = df.withColumn("duration_rank", rank().over(duration_window))

# Create column with the 'cheapest' and 'fastest'
df = df.withColumn("cheapest", when(col("price_rank") == 1, "Yes").otherwise("No"))
df = df.withColumn("fastest", when(col("duration_rank") == 1, "Yes").otherwise("No"))

# Remove excessive column now that cheapest and fastests is calculated
df = df.drop("duration_minutes", "price_rank", "duration_rank")

df.show()
