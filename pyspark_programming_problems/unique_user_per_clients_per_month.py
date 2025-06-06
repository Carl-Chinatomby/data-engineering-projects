"""
Problem Statement (Dell)

Write a query that returns the number of unique users per client per month.

Problem Breakdown
Steps:

- Extract the month and year from the time_id: This allows us to group the data by month and year.
- Count unique users: Using countDistinct to count the unique user_id for each client_id and month.
- Group by client_id and month: We group the data by client_id and the extracted month and year.
- Sort the result: Sort by client_id and month for better readability.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, countDistinct

spark = SparkSession.builder.appName("UniqueUsersPerClientPerMonth").getOrCreate()

data = [
    (1, '2020-02-28', '3668-QPYBK', 'Sendit', 'desktop', 'message sent', 3),
    (2, '2020-02-28', '7892-POOKP', 'Connectix', 'mobile', 'file received', 2),
    (3, '2020-04-03', '9763-GRSKD', 'Zoomit', 'desktop', 'video call received', 7),
    (4, '2020-04-02', '9763-GRSKD', 'Connectix', 'desktop', 'video call received', 7),
    (5, '2020-02-06', '9237-HQITU', 'Sendit', 'desktop', 'video call received', 7),
    (6, '2020-02-27', '8191-XWSZG', 'Connectix', 'desktop', 'file received', 2),
    (7, '2020-04-03', '9237-HQITU', 'Connectix', 'desktop', 'video call received', 7),
    (8, '2020-03-01', '9237-HQITU', 'Connectix', 'mobile', 'message received', 4),
    (9, '2020-04-02', '4190-MFLUW', 'Connectix', 'mobile', 'video call received', 7),
    (10, '2020-04-21', '9763-GRSKD', 'Sendit', 'desktop', 'file received', 2)
]
columns = ['id', 'time_id', 'user_id', 'customer_id', 'client_id', 'event_type', 'event_id']

df = spark.createDataFrame(data, columns)
df = df.withColumn('month_year', date_format(to_date('time_id', 'yyyy-MM-dd'), 'yyyy-MM'))
result_df = df.groupBy('client_id', 'month_year').agg(countDistinct('user_id').alias('unique_users'))
result_df = result_df.orderBy('client_id', 'month_year')
result_df.show(truncate=False)