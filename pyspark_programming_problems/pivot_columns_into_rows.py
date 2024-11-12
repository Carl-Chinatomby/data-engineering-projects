"""
Given Input -
StudentID, StudentName , AScore, BScore,CScore
123, A, 30, 31, 32
124, B, 40, 41, 42

Get the output in below format -
StudentID, StudentName , Subject , Score
123, A, AScore, 30
123, A, BScore, 31
123, A, CScore, 32
124, B, AScore, 40
124, B, BScore, 41
124, B, SScore, 42
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Transform Data") \
    .getOrCreate()

data = [
    (123, "A", 30, 31, 32),
    (124, "B", 40, 41, 42),
    (125, "B", 50, 51, 52)
]

df = spark.createDataFrame(data, ["StudentID", "StudentName", "AScore", "BScore", "CScore"])

pivot_df = df.selectExpr(
    "StudentID",
    "StudentName",
    "stack(3, 'AScore', AScore, 'BScore', BScore, 'CScore', CScore) as (Subject, Score)"
)

pivot_df.show()
