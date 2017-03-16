#!/DCNFS/applications/cdh/5.6/app/spark-1.5.0-cdh5.6.0/bin/spark-submit

from pyspark import SparkContext
from pyspark.sql import SQLContext
from scipy.spatial.distance import cosine

sc = SparkContext("local", "Simple App")
# Suppress log warnings to errors only, 
# doesn't apply right away but does the trick
sc.setLogLevel("ERROR")

ctx = SQLContext(sc)

#list of top 1000 subreddits
subreddits = set(sc.textFile("hdfs:///user/adiaztos/subreddits.txt") \
			 .collect())

# open all posts and get the subreddit and author columns
# remove any deleted authors
# filter only top subreddits
# create an RDD of author and subreddit tuples
# reduce the RDD by author and append the subreddit lists
# Save RDD as a text file
df = ctx.read.json("hdfs:///user/lhoward/posts_2016_all.json") \
		.select('subreddit', 'author') \
		.where("author != '[deleted]'").rdd \
		.filter(lambda x: x.subreddit in subreddits) \
		.distinct() \
		.map(lambda row: (row.author, [row.subreddit])) \
		.reduceByKey(lambda x,y: x+y) \
		.saveAsTextFile("hdfs:///user/adiaztos/subreddits_per_author.txt")

#print(df.collect())

# finding all unique subreddits
#subreddits = df.select("subreddit")
#subreddits = subreddits.distinct()

#subreddits.show()
#print(subreddits.count())
sc.stop()
