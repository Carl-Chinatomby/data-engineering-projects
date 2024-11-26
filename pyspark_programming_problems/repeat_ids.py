"""
Given Input —
id
1
2
3

Output —
id
1
2
2
3
3
3
"""
# NOTE THIS IS INCORRECT
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, explode, split

spark = SparkSession.builder \
    .appName("Repeat ID") \
    .getOrCreate()

df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

output_df = df.selectExpr("explode(array_repeat(id, int(id))) as id")

output_df.show()

spark.stop()
