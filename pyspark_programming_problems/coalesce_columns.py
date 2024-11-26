"""
From Given Input, Get the given output

Input —
first_name|middle_name|last_name
+ — — — — -+ — — — — — — -+ — — — — -+
John |null |Doe
null |Smith |null
null |"" |Robert

Output —
name
John
Smith
Robert
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("CoalesceColumns") \
    .getOrCreate()

data = [
    ("John", "", "Doe"),
    ("", "Smith", None),
    (None, "", "Robert")
]
schema = "col1 string, col2 string, col3 string"

df = spark.createDataFrame(data, schema)

# can't we redo a replace? Try this
df2 = df.withColumn("col1", when(col("col1") == '', None).otherwise(col("col1"))) \
    .withColumn("col2", when(col("col2") == '', None).otherwise(col("col2"))) \
    .withColumn("col3", when(col("col3") == '', None).otherwise(col("col3")))
# df2 = df.na.fill('') # opposite of this
df2 = df2.withColumn("name", coalesce(col("col1"), col("col2"), col("col3"))).select("name")
df2.show(truncate=False)

spark.stop()
