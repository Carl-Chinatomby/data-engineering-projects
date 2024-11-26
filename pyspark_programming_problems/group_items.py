"""
Convert the below input to the given output

Input —
name| item|weight
+— -+ — —- + — — -+
joe| mango| 2
kevin| kiwi| 2
joe|𝚋𝚊𝚗𝚊𝚗𝚊| 2
joe| mango| 3
kevin| 𝚝𝚊𝚌𝚘| 2
kevin| kiwi| 2

Output —
Joe (mango,5), (banana, 2)
Kevin (kiwi,4), (taco, 2)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("CollectListRows") \
    .getOrCreate()

data = [
    ("joe", "mango", 2),
    ("kevin", "kiwi", 2),
    ("joe", "banana", 2),
    ("joe", "mango", 3),
    ("kevin", "taco", 2),
    ("kevin", "kiwi", 2)
]
schema = ['name', 'item', 'weight']

df = spark.createDataFrame(data=data, schema=schema)

df_final = df.groupBy("name", "item").agg(sum("weight").alias("total_weight"))
df_final = df_final.groupBy("name").agg(collect_list(struct('item', "total_weight")).alias("total_items"))
df_final.show(truncate=False)

spark.stop()
