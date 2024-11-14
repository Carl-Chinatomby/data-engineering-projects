"""
Find the count of employees who are reportee to their Manager.

Given Input —
|Employee_Id|Manager_Id|
+ — — — — — -+ — — — — — +
| 1| null|
| 2| 1|
| 3| 8|
| 4| 2|
| 5| 2|
| 6| 3|
| 7| 4|

Output —
|Manager_Id|Employee_Count|
+ — — — — — + — — — — — — — +
| 1| 1|
| 3| 1|
| 8| 1|
| 2| 2|
| 4| 1|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
)

spark = SparkSession.builder \
    .appName("ManagerEmployeeCount") \
    .getOrCreate()

data = [
    (1, None),
    (2, 1),
    (3, 8),
    (4, 2),
    (5, 2),
    (6, 3),
    (7, 4)
]

columns = ["Employee_Id", "Manager_Id"]

df = spark.createDataFrame(data, columns)
df_filtered = df.filter(col("Manager_Id").isNotNull())

manager_count_df = df_filtered.groupBy("Manager_Id") \
    .agg(
        count("Employee_Id").alias("Employee_Count")
    )
manager_count_df.show()

spark.stop()
