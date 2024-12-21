"""
Given a dataset of user records containing duplicate user_id values. Each user
may have multiple rows, and our goal is to retain only the row with the latest
created_date for each user.
"""
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    desc,
    row_number,
)
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StructType,
    StructField,
    StringType,
)
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("RemoveDuplicates") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("email", StringType(), True),
])
data = [
    (1, "Alice", date(2023, 5, 10), "alice@example.com"),
    (1, "Alice", date(2023, 6, 15), "alice_new@example.com"),
    (2, "Bob", date(2023, 7, 1), "bob@example.com"),
    (3, "Charlie", date(2023, 5, 20), "charlie@example.com"),
    (3, "Charlie", date(2023, 6, 25), "charlie_updated@example.com"),
    (4, "David", date(2023, 8, 5), "david@example.com")
]

user_df = spark.createDataFrame(data, schema)
user_df.show()

# Define the window specification
window_spec = Window.partitionBy("user_id").orderBy(desc('created_date'))

# add a row number to each row based on window specification
ranked_df = user_df.withColumn("row_number", row_number().over(window_spec))

# Filter by row_number = 1
latest_records_df = ranked_df.filter(col("row_number") == 1).drop("row_number")

latest_records_df.show()
spark.stop()
