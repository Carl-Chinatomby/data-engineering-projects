"""
We have a table of hotels with various attributes, including the hotel name,
address, total reviews, and an average score based on the reviews. We need to
find the top 10 hotels with the highest average scores. The output should include:

The hotel name.
The average score of the hotel.
The records should be sorted by average score in descending order.
Solution Explanation
We can solve this problem step-by-step using PySpark:

- Read the Hotel Data: We’ll start by creating a DataFrame from the provided hotel data.
- Calculate the Average Score: We will use the avg() function to calculate the
average score of each hotel. Since the average_score column already contains the
average score, we can directly use it.
- Sort the Hotels by Score: After calculating the average scores, we will order the hotels in descending order of their average score.
- Limit to Top 10: Finally, we will use limit(10) to get the top 10 hotels based
on the highest ratings.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("TopHotelsByRating") \
    .getOrCreate()

data = [
    ('123 Ocean Ave, Miami, FL', 3, '2024-11-10', 4.2, 'Ocean View', 'American', 'Room small, but clean.', 5, 150, 'Great location and friendly staff!', 8, 30, 4.5, 'beachfront, family-friendly', '5 days', 25.7617, -80.1918),
    ('456 Mountain Rd, Boulder, CO', 2, '2024-11-12', 3.9, 'Mountain Lodge', 'Canadian', 'wifi slow.', 3, 120, 'nice rooms.', 10, 20, 4.0, 'scenic, nature', '3 days', 40.015, -105.2705),
    ('789 Downtown St, New York, NY', 5, '2024-11-15', 4.7, 'Central Park Hotel', 'British', 'Noisy, sleep.', 7, 200, 'Perfect location near Central Park.', 12, 50, 4.7, 'luxury, city-center', '1 day', 40.7831, -73.9712),
    ('101 Lakeside Blvd, Austin, TX', 1, '2024-11-08', 4.0, 'Lakeside Inn', 'Mexican', 'food avg.', 4, 80, 'Nice, friendly service.', 6, 15, 3.8, 'relaxing, family', '10 days', 30.2672, -97.7431),
    ('202 River Ave, Nashville, TN', 4, '2024-11-13', 4.5, 'Riverside', 'German', 'Limited parking', 2, 175, 'Great rooms.', 9, 25, 4.2, 'riverfront, peaceful', '2 days', 36.1627, -86.7816)
    ]
columns = ["hotel_address", "additional_number_of_scoring", "review_date", "average_score", "hotel_name", "reviewer_nationality", "negative_review", "review_total_negative_word_counts", "total_number_of_reviews", "positive_review", "review_total_positive_word_counts", "total_number_of_reviews_reviewer_has_given", "reviewer_score", "tags", "days_since_review", "lat", "lng"]

df = spark.createDataFrame(data, columns)
sorted_df = df.orderBy(col("average_score").desc())
top_hotels_df = sorted_df.select("hotel_name", "average_score").limit(10)

top_hotels_df.show(truncate=False)
spark.stop()
