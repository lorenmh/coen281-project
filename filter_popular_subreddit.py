from pyspark.sql import SQLContext
from pyspark.sql.functions import col

ALL_POSTS = 'hdfs:///user/lhoward/posts/2016_all.json'

popular_subreddits = [sr.strip() for sr in open('subreddits.txt').readlines()]
df = sqlContxt.read.json(ALL_POSTS)
popular = df.where(col("subreddit").isin(popular_subreddits))
popular.write.parquet("popular_subreddits_posts.parquet")
sqlContext.read.parquet("popular_subreddits_posts.parquet").show()
