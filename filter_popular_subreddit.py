from pyspark.sql import SQLContext
from pyspark.sql.functions import col

ALL_POSTS = 'hdfs:///user/lhoward/posts/2016_all.json'

# if you have a subreddits.txt file where names of popular subreddits are, uncomment next line
# popular_subreddits = [sr.strip() for sr in open('subreddits.txt').readlines()]
popular_subreddits = ['AskReddit', 'funny', 'todayilearned', 'science', 'worldnews', 'pics', 'IAmA',
                        'gaming', 'videos', 'movies']

df = sqlContxt.read.json(ALL_POSTS)
popular = df.where(col("subreddit").isin(popular_subreddits))

# save the data as spark parquet format
popular.write.parquet("popular_subreddits_posts.parquet")

# be able to read the parquets file
sqlContext.read.parquet("popular_subreddits_posts.parquet").show()
