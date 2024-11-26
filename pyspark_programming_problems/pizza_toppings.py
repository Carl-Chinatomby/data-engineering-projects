"""
Given a list of pizza toppings and their ingredient costs, find all possible 3-topping combinations and calculate their total cost. Display the combinations sorted by highest total cost first, and alphabetically by topping names for ties.

Given Input —
|topping_name|ingredient_cost|
+ — — — — — — + — — — — — — — -+
| Pepperoni| 0.5|
| Sausage| 0.7|
| Chicken| 0.55|
|Extra Cheese| 0.4|

Output —
|topping1 |topping2|topping3 |total_cost |
+ — — — — -+ — — — — + — — — — — — + — — — — — — — — — +
|Pepperoni|Sausage |Chicken |1.75 |
|Sausage |Chicken |Extra Cheese|1.65 |
|Pepperoni|Sausage |Extra Cheese|1.60|
|Pepperoni|Chicken |Extra Cheese|1.45|
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    concat_ws,
    sum as spark_sum,
    array,
    sort_array,
    collect_list,
)
from decimal import Decimal

spark = SparkSession.builder \
    .appName("PizzaToppings") \
    .getOrCreate()

toppings_data = [
    ("Pepperoni", Decimal(0.50)),
    ("Sausage", Decimal(0.70)),
    ("Chicken", Decimal(0.55)),
    ("Extra Cheese", Decimal(0.40))
]

toppings_columns = ["topping_name", "ingredient_cost"]
toppings_df = spark.createDataFrame(toppings_data,  toppings_columns)

# Self join the toppings DataFrame thrice to get all combinations of three toppings
comb_df = toppings_df.alias("t1").crossJoin(toppings_df.alias("t2")).crossJoin(toppings_df.alias("t3"))

# Filter out the combinations where toppings are the same (Maybe we may not want this)
# Comment out this line for the case where duplicate toppings count
comb_df = comb_df.filter((col("t1.topping_name") != col("t2.topping_name")) &
                         (col("t1.topping_name") != col("t3.topping_name")) &
                         (col("t2.topping_name") != col("t3.topping_name")))

comb_df = comb_df.select(
    col("t1.topping_name").alias("topping1"),
    col("t2.topping_name").alias("topping2"),
    col("t3.topping_name").alias("topping3"),
    (col("t1.ingredient_cost") + col("t2.ingredient_cost") + col("t3.ingredient_cost")).alias("total_cost")
)

comb_df = comb_df.withColumn("toppings", sort_array(array("topping1", "topping2", "topping3")))

comb_df = comb_df.withColumn("topping1", col("toppings")[0]) \
                 .withColumn("topping2", col("toppings")[1]) \
                 .withColumn("topping3", col("toppings")[2])

sorted_comb_df = comb_df.select("topping1", "topping2", "topping3", "total_cost") \
                        .distinct() \
                        .orderBy(col("total_cost").desc(), col("topping1"), col("topping2"), col("topping3"))

sorted_comb_df.show(truncate=False)

spark.stop()
