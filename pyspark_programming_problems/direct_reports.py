"""
From the given data set, Fetch the manager and their employees

Input —
employee_id|first_name|manager_id
4529| Nancy| 4125
4238| John| 4329
4329| Martina| 4125
4009| Klaus| 4329
4125| Mafalda| NULL
4500| Jakub| 4529
4118| Moira| 4952
4012| Jon| 4952
4952| Sandra| 4529
4444| Seamus| 4329

Output —
manager_id|manager_name|count
4125| Mafalda| 2
4952| Sandra| 2
4329| Martina| 3
4529| Nancy| 2
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("EmployeesWithManagers") \
    .getOrCreate()

data = [
    ('4529', 'Nancy', '4125'),
    ('4238','John', '4329'),
    ('4329', 'Martina', '4125'),
    ('4009', 'Klaus', '4329'),
    ('4125', 'Mafalda', 'NULL'),
    ('4500', 'Jakub', '4529'),
    ('4118', 'Moira', '4952'),
    ('4012', 'Jon', '4952'),
    ('4952', 'Sandra', '4529'),
    ('4444', 'Seamus', '4329'),
]
schema = ['employee_id', 'first_name', 'manager_id']

df = spark.createDataFrame(data=data, schema=schema)

# Self-join the DataFrame to get manager names
results_df = df.alias("e").join(df.alias("m"), col("e.manager_id") == col("m.employee_id"), "inner") \
    .select(
        col("e.employee_id"),
        col("e.first_name"),
        col("e.manager_id"),
        col("m.first_name").alias("manager_name")
    )

results_df.groupBy("manager_id", "manager_name").count().show()

spark.stop()
