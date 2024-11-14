"""
Read below json and print the required output

Input —
json_data = [
{
“sensorName”: “snx001”,
“sensorDate”: “2023–01–01”,
“sensorReadings”: [
{
“sensorChannel”: 1,
“sensorReading”: 3.7895084060850105,
“datetime”: “2020–01–01 00:00:00”
},
{
“sensorChannel”: 2,
“sensorReading”: 3.8490084060850105,
“datetime”: “2024–01–01 00:00:00”
}
]
}
]

Output —
sensorName | datetime | sensorChannel | sensorReading
+ — — — — — -+ — — — — + — — — — — — — -+ — — — — — — -+
snx001 | [2023–01–01 00:00:00, 2024–01–01 00:00:00] | [1, 2] | [3.7895084060850105, 3.8490084060850105]
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("FlattenJSON") \
    .getOrCreate()

json_data = [
    {
        "sensorName": "snx001",
        "sensorDate": "2023-01-01",
        "sensorReadings": [
            {
                "sensorChannel": 1,
                "sensorReading": 3.7895084060850105,
                "datetime": "2020-01-01 00:00:00"
            },
            {
                "sensorChannel": 2,
                "sensorReading": 3.8490084060850105,
                "datetime": "2024-01-01 00:00:00"
            }
        ]
    }
]

df = spark.read.json(spark.sparkContext.parallelize(json_data))
unnested_df = df.selectExpr("sensorName", "explode(sensorReadings) as sensorReadings")

output = unnested_df.selectExpr("sensorName", "sensorReadings.datetime as datetime",
    "sensorReadings.sensorChannel as sensorChannel",
    "sensorReadings.sensorReading as sensorReading")

output = output.groupBy('sensorName').agg(collect_list('datetime'), collect_list('sensorChannel'),
                                          collect_list('sensorReading'))
output.show(truncate=False)
