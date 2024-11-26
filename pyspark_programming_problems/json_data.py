"""
Read below json file —
[
{
“dept_id”: 102,
“e_id”: [
10201,
10202
]
},
{
“dept_id”: 101,
“e_id”: [
10101,
10102,
10103
]
}
]

output —
dept_id | e_id
101 | 10101
101 | 10102
101 | 10103
102 | 10201
102 | 10202
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('ReadJSON') \
    .getOrCreate()

df = spark.read.option("multiline", "true").json('data/sample_data.json')
df.printSchema()
df_exploded = df.selectExpr('dept_id', 'explode(e_id) as e_id')
df_exploded.show()
