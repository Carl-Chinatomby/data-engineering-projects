"""
Problem Statement (linkedin Hard-level)
We have a dataset of LinkedIn users, where each record contains details about
their work history — employer, job position, and the start and end dates of each job.

We want to find out how many users had Microsoft as their employer, and
immediately after that, they started working at Google, with no other employers
between these two positions.

Solution
Lets can break down the process into a few logical steps:

- Sort the data: First, we need to sort the data by user_id and start_date so that we can track the sequence of employers for each user.
- Use the LEAD function: The LEAD() function will help us look at the “next employer” for each user after Microsoft, which will tell us if their next employer is Google.
- Filter the results: After applying the LEAD() function, we filter the users where Microsoft is followed by Google.
- Count the distinct users: Finally, we count how many users meet the criteria.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lead,
)
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("LinkedInUsers") \
    .getOrCreate()

# Sample data for LinkedIn users
linkedin_data = [
    (1, 'Microsoft', 'developer', '2020-04-13', '2021-11-01'),
    (1, 'Google', 'developer', '2021-11-01', None),
    (2, 'Google', 'manager', '2021-01-01', '2021-01-11'),
    (2, 'Microsoft', 'manager', '2021-01-11', None),
    (3, 'Microsoft', 'analyst', '2019-03-15', '2020-07-24'),
    (3, 'Amazon', 'analyst', '2020-08-01', '2020-11-01'),
    (3, 'Google', 'senior analyst', '2020-11-01', '2021-03-04'),
    (4, 'Google', 'junior developer', '2018-06-01', '2021-11-01'),
    (4, 'Google', 'senior developer', '2021-11-01', None),
    (5, 'Microsoft', 'manager', '2017-09-26', None),
    (6, 'Google', 'CEO', '2015-10-02', None)
]

# Define the schema for the LinkedIn data
linkedin_columns = ['user_id', 'employer', 'position', 'start_date', 'end_date']

linkedin_df = spark.createDataFrame(linkedin_data, linkedin_columns)

# Step 1: Define window spec to order by start_date for each user_id
window_spec = Window.partitionBy("user_id").orderBy("start_date")

linkedin_with_next_employer = linkedin_df.withColumn('next_employer', lead('employer').over(window_spec))

# Step 3: Filter to find users who worked at Microsoft and then moved to Google
result = linkedin_with_next_employer.filter(
    (linkedin_with_next_employer.employer == 'Microsoft') &
    (linkedin_with_next_employer.next_employer == 'Google')
)

# Step 4: Select and display user_id
user_ids = result.select('user_id').distinct()

# Show the result
user_ids.show()

# Count the number of distinct user_ids
user_count = user_ids.count()

print(f"Number of users who worked at Microsoft and then moved to Google: {user_count}")
