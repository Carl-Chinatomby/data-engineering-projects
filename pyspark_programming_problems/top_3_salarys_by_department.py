"""
Problem Statement (Twitter Hard-level)

We have a table of employees that includes the following fields: id, first_name, last_name, age, sex, employee_title, department, salary, target, bonus, city, address, and manager_id. We need to find the top 3 distinct salaries for each department. The output should include:

The department name.
The top 3 distinct salaries for each department.
The results should be ordered alphabetically by department and then by the highest salary to the lowest salary.

Solution Explanation
We will solve this problem using the following steps in PySpark:

- Read the Employee Data: We’ll start by creating a DataFrame that contains the employee data.
- Select Distinct Salaries: Since we need distinct salaries, we will ensure we only select distinct salary values for each department.
- Rank Salaries: We will use PySpark’s Window function to rank the salaries within each department.
- Filter Top 3 Salaries: After ranking, we will filter out the top 3 salaries within each department.
- Sort the Results: Finally, we will order the results alphabetically by department and then by the salary in descending order.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TopSalariesByDepartments") \
    .getOrCreate()

# Sample data
data = [
    (1, 'Allen', 'Wang', 55, 'F', 'Manager', 'Management', 200000, 0, 300, 'California', '23St', 1),
    (13, 'Katty', 'Bond', 56, 'F', 'Manager', 'Management', 150000, 0, 300, 'Arizona', None, 1),
    (19, 'George', 'Joe', 50, 'M', 'Manager', 'Management', 100000, 0, 300, 'Florida', '26St', 1),
    (11, 'Richerd', 'Gear', 57, 'M', 'Manager', 'Management', 250000, 0, 300, 'Alabama', None, 1),
    (10, 'Jennifer', 'Dion', 34, 'F', 'Sales', 'Sales', 100000, 200, 150, 'Alabama', None, 13),
    (18, 'Laila', 'Mark', 26, 'F', 'Sales', 'Sales', 100000, 200, 150, 'Florida', '23St', 11),
    (20, 'Sarrah', 'Bicky', 31, 'F', 'Senior Sales', 'Sales', 200000, 200, 150, 'Florida', '53St', 19),
    (21, 'Suzan', 'Lee', 34, 'F', 'Sales', 'Sales', 130000, 200, 150, 'Florida', '56St', 19),
    (22, 'Mandy', 'John', 31, 'F', 'Sales', 'Sales', 130000, 200, 150, 'Florida', '45St', 19),
    (17, 'Mick', 'Berry', 44, 'M', 'Senior Sales', 'Sales', 220000, 200, 150, 'Florida', None, 11),
    (12, 'Shandler', 'Bing', 23, 'M', 'Auditor', 'Audit', 110000, 200, 150, 'Arizona', None, 11),
    (14, 'Jason', 'Tom', 23, 'M', 'Auditor', 'Audit', 100000, 200, 150, 'Arizona', None, 11),
    (16, 'Celine', 'Anston', 27, 'F', 'Auditor', 'Audit', 100000, 200, 150, 'Colorado', None, 11),
    (15, 'Michale', 'Jackson', 44, 'F', 'Auditor', 'Audit', 70000, 150, 150, 'Colorado', None, 11),
    (6, 'Molly', 'Sam', 28, 'F', 'Sales', 'Sales', 140000, 100, 150, 'Arizona', '24St', 13),
    (7, 'Nicky', 'Bat', 33, 'F', 'Sales', 'Sales', None, None, None, None, None, None)
]

# Define columns for the employees DataFrame
columns = ["id", "first_name", "last_name", "age", "sex", "employee_title", "department", "salary", "target", "bonus", "city", "address", "manager_id"]

df = spark.createDataFrame(data, columns)

# Step 1: Select distinct department and salaries
distinct_salaries_df = df.select("department", "salary").distinct()

# Step 2: Create a window spec for ranking salaries within each department
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Step 3: Rank salaries within each department
ranked_df = distinct_salaries_df.withColumn("rank", rank().over(window_spec))

# Step 4: Filter the top 3 salaries for each department
top_salaries_df = ranked_df.filter(col("rank") <= 3)

# Step 5: Sort results by department name and salary in desc order
results_df = top_salaries_df.orderBy("department", col("salary").desc())

results_df.show(truncate=False)
spark.stop()
