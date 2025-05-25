"""
Problem Statement (Pepsico"

You have two DataFrames: one containing employee information (employees)
and another containing department information (departments). Write a PySpark
function to join these DataFrames on the department ID and fill any missing
salary values with the average salary of the respective department.

Problem Breakdown
We use PySpark to join employee and department DataFrames while addressing missing salary values.

- groupBy(): To group employee data by department and calculate the average salary.
- agg() with avg() and round(): To compute and round the average salary to two decimal points.
- coalesce(): To fill missing salary values with the calculated average salary.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, round

spark = SparkSession.builder.appName("UniqueUsersPerClientPerMonth").getOrCreate()

employee_data = [
    (1, "Alice", 70000, 10),
    (2, "Bob", None, 20),  # Missing salary
    (3, "Charlie", 80000, 10),
    (4, "David", None, 30),  # Missing salary
    (5, "Eve", 75000, 20),
    (6, "Frank", 90000, 10),
    (7, "Grace", 52000, 30),
    (8, "Hannah", 62000, 20),
    (9, "Isaac", None, 30),  # Missing salary
    (10, "Jack", 71000, 20)
]
employee_columns = ["employee_id", "name", "salary", "department_id"]
# Sample data for departments
department_data = [
    (10, "Engineering"),
    (20, "HR"),
    (30, "Marketing")
]
department_columns = ["department_id", "department_name"]

employees_df = spark.createDataFrame(employee_data, employee_columns)
departments_df = spark.createDataFrame(department_data, department_columns)
avg_salary_df = employees_df.groupBy("department_id") \
    .agg(round(avg("salary"), 2).alias("average_salary"))

joined_df = employees_df.join(departments_df, "department_id", "left") \
    .join(avg_salary_df, "department_id", "left")

# Fill in missing values and select only necessary columns
final_df = joined_df.withColumn(
    "salary",
    coalesce(joined_df.salary, joined_df.average_salary)
).select("employee_id", "name", "salary", "department_name") \
    .orderBy("department_name", "employee_id")
final_df.show()
spark.stop()
