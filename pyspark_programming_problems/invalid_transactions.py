"""
Bank of Ireland has requested that you detect invalid transactions in December 2022.
An invalid transaction is one that occurs outside of the bank’s normal business hours.
The following are the hours of operation for all branches:

Monday — Friday: 09:00–16:00
Saturday & Sunday: Closed
Irish Public Holidays: 25th and 26th December

You need to find the transaction IDs of all invalid transactions that occurred in December 2022.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, month, year, hour, minute, dayofmonth

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("InvalidTransactions") \
    .getOrCreate()

# Sample data
data = [
    (1051, '2022-12-03 10:15'),
    (1052, '2022-12-03 17:00'),
    (1053, '2022-12-04 10:00'),
    (1054, '2022-12-04 14:00'),
    (1055, '2022-12-05 08:59'),
    (1056, '2022-12-05 16:01'),
    (1057, '2022-12-06 09:00'),
    (1058, '2022-12-06 15:59'),
    (1059, '2022-12-07 12:00'),
    (1060, '2022-12-08 09:00'),
    (1061, '2022-12-09 10:00'),
    (1062, '2022-12-10 11:00'),
    (1063, '2022-12-10 17:30'),
    (1064, '2022-12-11 12:00'),
    (1065, '2022-12-12 08:59'),
    (1066, '2022-12-12 16:01'),
    (1067, '2022-12-25 10:00'),
    (1068, '2022-12-25 15:00'),
    (1069, '2022-12-26 09:00'),
    (1070, '2022-12-26 14:00'),
    (1071, '2022-12-26 16:30'),
    (1072, '2022-12-27 09:00'),
    (1073, '2022-12-28 08:30'),
    (1074, '2022-12-29 16:15'),
    (1075, '2022-12-30 14:00'),
    (1076, '2022-12-31 10:00')
]

columns = ["transaction_id", "timestamp"]
df = spark.createDataFrame(data, columns)

# Filter transactions to December 2022
df_dec_2022 = df.filter((month(col('timestamp')) == 12) & (year(col('timestamp')) == 2022))

weekend_check = (dayofweek(col('timestamp'))).isin([1, 7])
outside_business_hours = (hour(col('timestamp')) < 9) | (hour(col('timestamp')) > 16) | ((hour(col('timestamp')) == 16) & (minute(col('timestamp')) > 0))
public_holidays = (dayofmonth(col('timestamp')) == 25) | (dayofmonth(col('timestamp')) == 26)

invaid_transactions = df_dec_2022.filter(weekend_check | outside_business_hours | public_holidays)

invaid_transactions.show()
