from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, trim, desc


# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending

hashtag_counts = (
    posts_df
    .withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
    .withColumn("Hashtag", lower(trim(col("Hashtag"))))
    .filter(col("Hashtag") != "")
    .groupBy("Hashtag")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)


# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
