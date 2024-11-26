"""
From the below data, find the total hours employee was inside office.

Input —
emp_id| punch_time|flag
11114|1900–01–01 08:30:00| I
11114|1900–01–01 10:30:00| O
11114|1900–01–01 11:30:00| I
11114|1900–01–01 15:30:00| O
11115|1900–01–01 09:30:00| I
11115|1900–01–01 17:30:00| O
"""
import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    StringType,
)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("TotalInTime") \
    .getOrCreate()

data = [
    (11114, datetime.datetime.strptime('08:30:00.00', "%H:%M:%S.%f"), "I"),
    (11114, datetime.datetime.strptime('10:30:00.00', "%H:%M:%S.%f"), 'O'),
    (11114, datetime.datetime.strptime('11:30:00.00', "%H:%M:%S.%f"), 'I'),
    (11114, datetime.datetime.strptime('15:30:00.00', "%H:%M:%S.%f"), 'O'),
    (11115, datetime.datetime.strptime('09:30:00.00', "%H:%M:%S.%f"), 'I'),
    (11115, datetime.datetime.strptime('17:30:00.00', "%H:%M:%S.%f"), 'O')
]

schema = StructType([
    StructField('emp_id', LongType(), True),
    StructField('punch_time', TimestampType(), True),
    StructField('flag', StringType(), True)
])

df = spark.createDataFrame(data, schema)

window_agg = Window.partitionBy('emp_id').orderBy(col('punch_time'))

df = df.withColumn('prev_time', lag(col('punch_time')).over(window_agg))
df = df.withColumn('time_diff', (col('punch_time').cast('long') - col('prev_time').cast('long'))/3600)


df = df.groupBy('emp_id').agg(sum(when(col('flag') == 'O', col('time_diff')).otherwise(0)).alias('total_time'))
df.show()

spark.stop()
