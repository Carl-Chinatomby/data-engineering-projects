"""
Input —
col1|col2|col3
alpha| aa| 1
alpha| aa| 2
beta| bb| 3
beta| bb| 4
beta| bb| 5

Output —
col1|col2|col3_list
alpha| aa| [1, 2]
beta| bb|[3, 4, 5]
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('GroupRows') \
    .getOrCreate()

data = [
    ("alpha", "aa", 1),
    ("alpha", "aa", 2),
    ("beta", "bb", 3),
    ("beta", "bb", 5),
    ("beta", "bb", 4),
]
schema = ['col1', 'col2', 'col3']

df = spark.createDataFrame(data, schema=schema)
df.show()
df_grouped = df.groupBy('col1', 'col2') \
    .agg(collect_list('col3')) \
    .alias('col3_list')
df_grouped.show()

spark.stop()
