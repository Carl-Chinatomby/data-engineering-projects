"""
Find the job titles of the highest-paid employees. output should include the
highest-paid title or multiple titles with the same salary.

Input —

worker:
|worker_id|first_name|last_name|salary|joining_date| department|
| 1| John| Doe| 10000| 2023–01–01|Engineering|
| 2| Jane| Smith| 12000| 2022–12–01| Marketing|
| 3| Alice| Johnson| 12000| 2022–11–01|Engineering|

title:
|worker_ref_id|worker_title|affected_from|
| 1| Engineer| 2022–01–01|
| 2| Manager| 2022–01–01|
| 3| Engineer| 2022–01–01|

Output —
|worker_id|first_name|last_name|best_paid_title|salary|
| 3| Alice| Johnson| Engineer| 12000|
| 2| Jane| Smith| Manager| 12000|
"""
from pyspark.sql import (
    SparkSession,
    SQLContext,
    Window
)
from pyspark.sql.functions import rank

spark = SparkSession.builder \
    .appName('HighestPaidJobTitles') \
    .getOrCreate()

worker_data = [
    (1, 'John', 'Doe', 10000, '2023-01-01', 'Engineering'),
    (2, 'Jane', 'Smith', 12000, '2022-12-01', 'Marketing'),
    (3, 'Alice', 'Johnson', 12000, '2022-11-01', 'Engineering'),
]
columns = ['worker_id', 'first_name', 'last_name', 'salary', 'joining_date', 'department']
worker = spark.createDataFrame(worker_data, columns)

title_data = [
    (1, 'Engineer', '2022-01-01'),
    (2, 'Manager', '2022-01-01'),
    (3, 'Engineer', '2022-01-01'),
]
columns = ['worker_ref_id', 'worker_title', 'affected_from']
title = spark.createDataFrame(title_data, columns)

joined_df = worker.join(title, worker.worker_id == title.worker_ref_id)

ranked_df = joined_df.withColumn('salary_rank',
    rank().over(Window.orderBy(joined_df['salary'].desc())))
highest_paid_df = ranked_df.filter(ranked_df['salary_rank'] == 1)
result_df = highest_paid_df.select(
    'worker_id', 'first_name', 'last_name', 'worker_title', 'salary'
    ).withColumnRenamed('worker_title', 'best_paid_title')
result_df.show()

spark.stop()
