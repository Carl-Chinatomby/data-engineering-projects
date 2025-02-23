"""
Problem Statement:
We need to find the genre of the person with the most Oscar wins.
In case of a tie (multiple people with the same number of wins),
we need to return the person who comes first alphabetically by their name.

Problem Breakdown
We will join two tables: nominee_information (contains details about the nominees)
and oscar_nominees (contains the Oscar nominations and whether the nominee won).
The primary goal is to count the total number of wins and identify the nominee
with the highest count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = SparkSession.builder \
    .appName("OscarWins") \
    .getOrCreate()

# Sample data for nominee_information
nominee_data = [
    ('Jennifer Lawrence', 'P562566', 'Drama', '1990-08-15', 755),
    ('Jonah Hill', 'P418718', 'Comedy', '1983-12-20', 747),
    ('Anne Hathaway', 'P292630', 'Drama', '1982-11-12', 744),
    ('Jennifer Hudson', 'P454405', 'Drama', '1981-09-12', 742),
    ('Rinko Kikuchi', 'P475244', 'Drama', '1981-01-06', 739)
]

# Sample data for oscar_nominees
oscar_data = [
    (2008, 'actress in a leading role', 'Anne Hathaway', 'Rachel Getting Married', 0, 77),
    (2012, 'actress in a supporting role', 'Anne HathawayLes', 'Mis_rables', 1, 78),
    (2006, 'actress in a supporting role', 'Jennifer Hudson', 'Dreamgirls', 1, 711),
    (2010, 'actress in a leading role', 'Jennifer Lawrence', 'Winters Bone', 1, 717),
    (2012, 'actress in a leading role', 'Jennifer Lawrence', 'Silver Linings Playbook', 1, 718),
    (2011, 'actor in a supporting role', 'Jonah Hill', 'Moneyball', 0, 799),
    (2006, 'actress in a supporting role', 'Rinko Kikuchi', 'Babel', 0, 1253)
]

# Define schema for nominee_information
columns_nominee = ["name", "amg_person_id", "top_genre", "birthday", "id"]

# Define schema for oscar_nominees
columns_oscar = ["year", "category", "nominee", "movie", "winner", "id"]

df_nominee = spark.createDataFrame(nominee_data, columns_nominee)
df_oscar = spark.createDataFrame(oscar_data, columns_oscar)

win_counts = df_oscar.filter(df_oscar.winner == 1).groupBy("nominee").agg(count("*").alias("total_wins"))

joined_df = win_counts.join(df_nominee, win_counts['nominee'] == df_nominee['name'])

sorted_df = joined_df.orderBy(desc("total_wins"), "name")
result = sorted_df.select("name", "top_genre").first()
print(result['top_genre'], result['name'])