"""
Write a word count function using pyspark that covers following steps —
1. read the data from the input.txt file
2. lower the words
3. remove any punctuations
4. Remove the None, blank strings and digits
5. sort the word sin descending order
6. write it to a csv file

Test Input —
test_data = [
“Hello world! This is a test.”,
“Spark is awesome, isn’t it?”,
“Test test test.”
“Test 0 21.”
]

Output —
word|count
test| 4
is| 2
a| 1
awesome| 1
spark| 1
this| 1
world| 1
hello| 1
isn| 1
it| 1
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

#output_file_path = "output_word_count_result.csv"

spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

test_data = [
    "Hello world! This is a test.",
    "Spark is awesome, isn't it?",
    "Test test test."
    "Test 0 21."
]

input_df = spark.createDataFrame(test_data, StringType())
# Split into lines then lines into words
input_df = input_df.select(split(col("value"), " ").alias("line"))
input_df = input_df.select(explode(col("line")).alias("value"))

# lower the words
input_df =  input_df.withColumn("value", lower(col("value")))

# split text into words, remove punctuations
words_df = input_df.select(regexp_extract(col("value"), "[a-z]+", 0).alias("word"))

# Remove none, blank strings, digit values
words_df =words_df.filter(col("word").isNotNull() & (col("word") != "") & (~col("value").rlike("^\d+$")))

# Perform word count
word_count_df = words_df.groupBy("word").count()

# sort the words in desc order
word_count_df = word_count_df.orderBy(col("count").desc())
word_count_df.show()

# prep for writing
#word_count_df.coalesce(1).show().write_csv(output_file_path, header=True)


spark.stop()
