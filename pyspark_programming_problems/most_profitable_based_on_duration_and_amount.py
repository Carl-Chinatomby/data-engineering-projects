"""
Problem Statement (Uber Hard-level)

Let’s Imagine we are working at Uber, and our task is to determine the most
profitable location based on signup duration and transaction amounts.
We are provided with two datasets: one containing signup details
(including start and stop times) and another containing transaction
details (such as amounts).

Solution Explanation
Our goal is to calculate:
- The average signup duration in minutes for each location.
- The average transaction amount for each location.
- The ratio of the average transaction amount to the average signup duration.
- Sort the results by the highest ratio to identify the most profitable location.

Let’s break down how we can achieve this in PySpark.

Solution Explanation
Step 1: Calculate Signup Duration
To calculate the signup duration for each location, we need to find the
difference between the signup_stop_date and signup_start_date. The result will
be in seconds, so we divide by 60 to get the duration in minutes.

Step 2: Calculate Average Transaction Amount
We group the transactions by signup_id and calculate the average transaction
amount for each signup. This will allow us to associate the average transaction
amount with each signup, which can later be grouped by location.

Step 3: Combine Signup and Transaction Data
Next, we join the signup data (which includes the duration) with the transaction
data (which includes the average transaction amount). By doing so, we can easily
compute the necessary metrics by location.

Step 4: Calculate Ratio
We calculate the ratio of average transaction amount to average signup duration
for each location. To avoid division by zero errors, we use a conditional check
that returns zero if the signup duration is zero.

Step 5: Group and Sort Results
Finally, we group the data by location, compute the average signup duration and
transaction amount, and then sort the locations by the ratio of transaction
amount to signup duration, in descending order. This gives us the most
profitable location at the top.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, avg

spark = SparkSession.builder \
    .appName("UberProfitableLocation") \
    .getOrCreate()

# Sample Data - Creating the DataFrames for signups and transactions
data_signups = [
    (1, '2020-01-01 10:00:00', '2020-01-01 12:00:00', 101, 'New York'),
    (2, '2020-01-02 11:00:00', '2020-01-02 13:00:00', 102, 'Los Angeles'),
    (3, '2020-01-03 10:00:00', '2020-01-03 14:00:00', 103, 'Chicago'),
    (4, '2020-01-04 09:00:00', '2020-01-04 10:30:00', 101, 'San Francisco'),
    (5, '2020-01-05 08:00:00', '2020-01-05 11:00:00', 102, 'New York')
]
columns_signups = ["signup_id", "signup_start_date", "signup_stop_date", "plan_id", "location"]

data_transactions = [
    (1, 1, '2020-01-01 10:30:00', 50.00),
    (2, 1, '2020-01-01 11:00:00', 30.00),
    (3, 2, '2020-01-02 11:30:00', 100.00),
    (4, 2, '2020-01-02 12:00:00', 75.00),
    (5, 3, '2020-01-03 10:30:00', 120.00),
    (6, 4, '2020-01-04 09:15:00', 80.00),
    (7, 5, '2020-01-05 08:30:00', 90.00)
]
columns_transactions = ["transaction_id", "signup_id", "transaction_start_date", "amt"]

signups_df = spark.createDataFrame(data_signups, columns_signups)
transactions_df = spark.createDataFrame(data_transactions, columns_transactions)

# 1. Calculate signup duration in minutes for each signup
signups_df = signups_df.withColumn(
    "signup_duration_minutes",
    (unix_timestamp("signup_stop_date")-unix_timestamp("signup_start_date"))/60
)

# 2. Calculate average transaction amount for each signup
transaction_avg_df = transactions_df.groupBy("signup_id").agg(avg("amt").alias("avg_transaction_amount"))

# 3. Join the signups with transaction averages
joined_df = signups_df.join(transaction_avg_df, on="signup_id", how="inner")


# 4. Group by location and calculate average duration, average transaction amount, and ratio
result_df = joined_df.groupBy("location").agg(
    avg("signup_duration_minutes").alias("avg_duration"),
    avg("avg_transaction_amount").alias("avg_transaction_amount")
)

# 5. Calculate ratio of transaction amount to signup duration
result_df = result_df.withColumn(
    "ratio",
    when(col("avg_duration") != 0, col("avg_transaction_amount") / col("avg_duration")).otherwise(0)
)
# 6. Sort by ratio from highest to lowest
result_df = result_df.orderBy(col("ratio"), ascending=False)

# Show the final result
result_df.show(truncate=False)

spark.stop()

