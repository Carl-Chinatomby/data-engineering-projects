"""
Given input which has customer’s debit and credit details. Find the total account balance for a Customer_no (Credit-Debit)

Input —
Customer_No,Card_type,Date,Category,Transaction_Type,Amount
+ — — — — — -+ — — — —+ — — -+ — — —+ — — — — — — — — -+ — — — — +
1000501,Platinum Card,1/1/2018,Shopping,debit,11.11
1000501,Checking,1/2/2018,Mortgage & Rent,debit,1247.44
1000501,Platinum Card,1/11/2018,Groceries,debit,43.54
1000501,Checking,1/12/2018,Paycheck,credit,2000
1000531,Platinum Card,1/13/2018,Fast Food,debit,32.91
1000531,Platinum Card,1/13/2018,Shopping,debit,39.05
1000531,Silver Card,1/15/2018,Groceries,debit,44.19
1000654,Silver Card,1/29/2018,Gas & Fuel,debit,30.42
1000654,Silver Card,1/29/2018,Restaurants,debit,25
1000654,Platinum Card,2/1/2018,Shopping,debit,11.11
1000654,Checking,2/2/2018,Mortgage & Rent,debit,1247.44
1000654,Silver Card,2/6/2018,Credit Card Payment,credit,154.13
1001863,Checking,2/7/2018,Credit Card Payment,debit,154.13
1001863,Checking,2/7/2018,Utilities,debit,65
1001863,Platinum Card,2/9/2018,Haircut,debit,30
1001863,Platinum Card,2/11/2018,Restaurants,debit,106.8
1001863,Silver Card,2/12/2018,Gas & Fuel,debit,36.47
1002324,Checking,3/12/2018,Mobile Phone,debit,89.52
1002324,Platinum Card,3/15/2018,Coffee Shops,debit,3.5
1002324,Checking,3/15/2018,Utilities,debit,60
1002324,Checking,3/16/2018,Paycheck,credit,2000
1002324,Silver Card,3/17/2018,Alcohol & Bars,debit,19.5
1000210,Platinum Card,3/17/2018,Fast Food,debit,23.34
1000210,Silver Card,3/19/2018,Restaurants,debit,36.48
1000210,Checking,3/19/2018,Utilities,debit,35
1000210,Silver Card,3/22/2018,Gas & Fuel,credit,30.55
1000210,Platinum Card,3/23/2018,Credit Card Payment,credit,559.91

Output —
|Customer_No| total|
+ — — — — — -+ — — — — +
| 1001863| null|
| 1000210| 495.63999999999993|
| 1000531| null|
| 1000654|-1159.8400000000001|
| 1002324| 1827.48|
| 1000501| 697.9100000000001|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    DoubleType,
    StringType,
)

spark = SparkSession.builder\
        .appName("CustomerTransaction")\
        .getOrCreate()

df = spark.read.csv("data/customer_transaction.csv", header=True)

df = df.withColumn('Amount', df['Amount'].cast(DoubleType()))
df = df.withColumn("amount_sub",
                    when(col("Transaction_Type") == "debit", -1*col("Amount")).otherwise(col("Amount")))
df = df.groupby("Customer_No").pivot("Transaction_Type").agg(sum("Amount"))
df = df.withColumn("total", df.credit-df["debit"]).drop("credit", "debit")
df.show()

spark.stop()