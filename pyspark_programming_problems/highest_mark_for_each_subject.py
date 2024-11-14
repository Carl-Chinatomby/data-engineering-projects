"""
Fetch students with highest mark in each subject

Input —
Sub|Name|Marks
+— — -+ — —+ — — -+
Eng|John|85
Math|John|76
Science|John|89
Eng|Maria|91
Math|Maria|74
Science|Maria|82
Eng|Karthik|91
Math|Karthik|100
Science|Karthik|76

Output —
| Sub| Name|Marks|Rank|
+ — — -+ — — + — — -+ — — -+
|Science| John| 89| 89|
| Eng| Maria| 91| 91|
| Eng|Karthik| 91| 91|
| Math|Karthik| 100| 100|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("HighestMarksBySubject") \
    .getOrCreate()

data = [
    ("Eng", "John", 85),
    ("Math", "John", 76),
    ("Science", "John", 89),
    ("Eng", "Maria", 91),
    ("Math", "Maria", 74),
    ("Science", "Maria", 82),
    ("Eng", "Karthik", 91),
    ("Math", "Karthik", 100),
    ("Science", "Karthik", 76)
]
df = spark.createDataFrame(data, ["Sub", "Name", "Marks"])

window_spec = Window.partitionBy("Sub").orderBy(df["Marks"].desc())
results_df = df.withColumn("Rank", max("Marks").over(window_spec)) \
    .filter(col("Marks") == col("Rank")) \
    .drop("Rank")

results_df.show()

spark.stop()
