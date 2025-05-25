"""
Problem Statement (Meta)"

Given a dataset of employee details, find the highest salary that appears only once.

Our output should display the highest salary among the salaries that appear exactly once in the dataset.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max

spark = SparkSession.builder.appName("HighestSalaryAppearingOnce").getOrCreate()

data = [
    (5, 'Max', 'George', 26, 'M', 'Sales', 'Sales', 1300, 200, 150, 'Max@company.com', 'California', '2638 Richards Avenue', 1),
    (13, 'Katty', 'Bond', 56, 'F', 'Manager', 'Management', 150000, 0, 300, 'Katty@company.com', 'Arizona', None, 1),
    (11, 'Richerd', 'Gear', 57, 'M', 'Manager', 'Management', 250000, 0, 300, 'Richerd@company.com', 'Alabama', None, 1),
    (10, 'Jennifer', 'Dion', 34, 'F', 'Sales', 'Sales', 1000, 200, 150, 'Jennifer@company.com', 'Alabama', None, 13),
    (19, 'George', 'Joe', 50, 'M', 'Manager', 'Management', 250000, 0, 300, 'George@company.com', 'Florida', '1003 Wyatt Street', 1),
    (18, 'Laila', 'Mark', 26, 'F', 'Sales', 'Sales', 1000, 200, 150, 'Laila@company.com', 'Florida', '3655 Spirit Drive', 11),
    (20, 'Sarrah', 'Bicky', 31, 'F', 'Senior Sales', 'Sales', 2000, 200, 150, 'Sarrah@company.com', 'Florida', '1176 Tyler Avenue', 19)
]
columns = ["id", "first_name", "last_name", "age", "sex", "employee_title", "department", "salary", "target", "bonus", "email", "city", "address", "manager_id"]

df = spark.createDataFrame(data, columns)

salary_count_df = df.groupBy("salary").agg(count("salary").alias("salary_count"))
unique_salaries_df = salary_count_df.filter("salary_count = 1")
highest_unique_salary_df = unique_salaries_df.agg(max("salary").alias("highest_unique_salary"))
highest_unique_salary_df.show()
spark.stop()
