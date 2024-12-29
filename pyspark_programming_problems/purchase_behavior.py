"""
Problem Statement
IBM is working on a new feature to analyze user purchasing behavior for all Fridays in the first quarter of the year. For each Friday separately, calculate the average amount users have spent per order. The output should contain the week number of that Friday and average amount spent.

Steps:
1. Filter for Fridays in the first quarter of the year (Q1): We’ll filter the user_purchases table for Fridays that fall within the first quarter of the year (January, February, and March).
2. Calculate the Week Number: We’ll calculate the week number for each Friday (using the date).
3. Group by Week Number: We’ll group the data by the week number and compute the average amount spent for each Friday in that week.
4. Output the results: The final output will include the week number and the average amount spent.

PySpark Approach:
Filter Fridays in Q1: First, we need to identify the Fridays (where day_name == 'Friday') and restrict the data to the first quarter of the year (dates from January to March).
Calculate Week Number: We will use the weekofyear() function in PySpark to extract the week number from the date.
Group and Aggregate: We group the data by week number and compute the average of amount_spent for each week.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("UserPurchaseQ1Fridays") \
    .getOrCreate()

# Sample data for user purchases
user_purchases_data = [
    (1047, '2023-01-01', 288, 'Sunday'),
    (1099, '2023-01-04', 803, 'Wednesday'),
    (1055, '2023-01-07', 546, 'Saturday'),
    (1040, '2023-01-10', 680, 'Tuesday'),
    (1052, '2023-01-13', 889, 'Friday'),
    (1052, '2023-01-13', 596, 'Friday'),
    (1016, '2023-01-16', 960, 'Monday'),
    (1023, '2023-01-17', 861, 'Tuesday'),
    (1010, '2023-01-19', 758, 'Thursday'),
    (1013, '2023-01-19', 346, 'Thursday'),
    (1069, '2023-01-21', 541, 'Saturday'),
    (1030, '2023-01-22', 175, 'Sunday'),
    (1034, '2023-01-23', 707, 'Monday'),
    (1019, '2023-01-25', 253, 'Wednesday'),
    (1052, '2023-01-25', 868, 'Wednesday'),
    (1095, '2023-01-27', 424, 'Friday'),
    (1017, '2023-01-28', 755, 'Saturday'),
    (1010, '2023-01-29', 615, 'Sunday'),
    (1063, '2023-01-31', 534, 'Tuesday'),
    (1019, '2023-02-03', 185, 'Friday'),
    (1019, '2023-02-03', 995, 'Friday'),
    (1092, '2023-02-06', 796, 'Monday'),
    (1058, '2023-02-09', 384, 'Thursday'),
    (1055, '2023-02-12', 319, 'Sunday'),
    (1090, '2023-02-15', 168, 'Wednesday'),
    (1090, '2023-02-18', 146, 'Saturday'),
    (1062, '2023-02-21', 193, 'Tuesday'),
    (1023, '2023-02-24', 259, 'Friday')
]

# Define schema for user purchases
columns_user_purchases = ["user_id", "date", "amount_spent", "day_name"]

user_purchases_df = spark.createDataFrame(
    user_purchases_data,
    columns_user_purchases
)

# Step 1: Filter the data for Fridays in the first quarter (Q1) of the year
fridays_q1_df = user_purchases_df.filter(
    (col("day_name") == "Friday") &
    (col("date").between("2023-01-01", "2023-03-31"))
)

# Step 2: Add a new column for the week number using the `weekofyear()` function
fridays_q1_df = fridays_q1_df.withColumn("week_number", weekofyear(col("date")))

# Step 3: Group by week number and calculate the average amount spent
avg_spend_per_week_df = fridays_q1_df.groupBy("week_number") \
    .agg({"amount_spent": "avg"}) \
    .withColumnRenamed("avg(amount_spent)", "avg_amount_spent")

avg_spend_per_week_df.show()
spark.stop()
