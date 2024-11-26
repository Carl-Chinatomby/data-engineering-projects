"""
You have been asked to find the employees with the highest and lowest salary
from the below sample data for worker and title. The output includes a column
salary_type that categorizes the output by:

‘Highest Salary’ represents the highest salary
‘Lowest Salary’ represents the lowest salary

worker:
|worker_id|first_name|last_name|salary|joining_date| department|
| 1| John| Doe| 5000| 2023–01–01|Engineering|
| 2| Jane| Smith| 6000| 2023–01–15| Marketing|
| 3| Alice| Johnson| 4500| 2023–02–05|Engineering|

title:
|worker_ref_id|worker_title|effective_from|
| 1| Engineer| 2022–01–01|
| 2| Manager| 2022–01–01|
| 3| Engineer| 2022–01–01|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    max as spark_max,
    min as spark_min,
    when,
    lit,
)

spark = SparkSession.builder \
    .appName('HighetLowestSalaryEmployees') \
    .getOrCreate()

worker_data = [
    (1, 'John', 'Doe', 5000, '2023-01-01', 'Engineering'),
    (2, 'Jane', 'Smith', 6000, '2023-01-15', 'Marketing'),
    (3, 'Alice', 'Johnson', 4500, '2023-02-05', 'Engineering')
]
title_data = [
    (1, 'Engineer', '2022-01-01'),
    (2, 'Manager', '2022-01-01'),
    (3, 'Engineer', '2022-01-01')
]
worker_columns = ['worker_id', 'first_name', 'last_name', 'salary', 'joining_date', 'department']
title_columns = ['worker_ref_id', 'worker_title', 'effective_from']
worker_df = spark.createDataFrame(worker_data, worker_columns)
title_df = spark.createDataFrame(title_data, title_columns)

joined_df = worker_df.join(title_df, worker_df.worker_id == title_df.worker_ref_id, 'inner')
# NOTE: pyspark doensn't allow you to do this directly, so I do the query separate for the case
# statement below
max_salary = worker_df.agg(spark_max("salary").alias('max_salary')).take(1)[0]['max_salary']
min_salary =  worker_df.agg(spark_min("salary").alias('min_salary')).take(1)[0]['min_salary']

result_df = joined_df.withColumn("salary_type",
                when(joined_df["salary"] == max_salary, "Highest Salary")
                .when(joined_df["salary"] == min_salary, "Lowest Salary")
                .otherwise(None))

result_df.select("worker_id", "first_name", "last_name", "salary","department", "salary_type").show()

spark.stop()


"""
SELECT w.worker_id, w.first_name, w.last_name, t.worker_title, w.department, salary,
CASE
WHEN salary = (SELECT MAX(salary) AS max_salary FROM worker) THEN 'Highest Salary'
WHEN salary = (SELECT MIN(salary) AS min_salary FROM worker) THEN 'Lowest Salary'
END AS salary_tpye
FROM
    worker w
        INNER JOIN
    title t
        ON w.worker_id = t.worker_ref_id
;
"""
