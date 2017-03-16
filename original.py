from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.mllib.linalg import SparseVector
from pyspark.sql.functions import col, explode, monotonicallyIncreasingId

ord_A = ord('A')
ord_Z = ord('Z')
ord_a = ord('a')
ord_z = ord('z')
ord_0 = ord('0')
ord_9 = ord('9')
ord_sp = ord(' ')

def allowed_ord(o):
	return (
		(o>=ord_A and o<=ord_Z) or
		(o>=ord_a and o<=ord_z) or
		(o>=ord_0 and o<=ord_9)
	)


def tokenizer(d):
	# convert to ascii
	ords = [ord(c) for c in d]
	# convert non \w to space
	ords = [o if allowed_ord(o) else ord_sp for o in ords]
	# lowercase all upcase
	ords = [o+32 if (o<=ord_Z and o>=ord_A) else o for o in ords]
	# state-machine to accumulate tokens
	len_ords = len(ords)
	start_index = 0
	accumulator = []
	for i in xrange(len_ords):
		if ords[i]==ord_sp:
			if i-start_index > 0:
				accumulator.append(''.join(chr(o) for o in ords[start_index:i]))
			start_index = i+1
	if start_index < len_ords-1:
		accumulator.append(''.join(chr(o) for o in ords[start_index:len_ords]))
	return accumulator

def ntoken(n, l):
	len_l = len(l)
	if len_l == 0:
		return [None]
	if len_l == 1:
		return [l[0]]
	if len_l <= n:
		return [' '.join(l)]
	accumulator = []
	for i in xrange(len_l-n+1):
		accumulator.append(' '.join(l[i:i+n]))
	return accumulator

sc = SparkContext('local', 'reddit_vector')
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

df = sqlContext.read.json('hdfs:///user/lhoward/posts_2016_filtered.json')

subreddit_tokens = (
	df
		.select('subreddit', 'title')
		.map(lambda p: Row(subreddit=p[0], tokens=tokenizer(p[1])))
		.toDF()
)

subreddit_ngrams = (
	subreddit_tokens
		.select('subreddit', 'tokens')
		.map(lambda p: Row(subreddit=p[0], ngrams=ntoken(2, p[1])))
		.toDF()
)

subreddit_ngrams_exploded = (
	subreddit_ngrams
		.select(
			'subreddit',
			explode('ngrams').alias('ngram')
		)
		.where(col('ngram').isNotNull())
		.distinct()
)

ngrams = (
	subreddit_ngrams_exploded
		.select('ngram')
		.distinct()
)

ngram_index_row = Row('ngram', 'index')

ngram_indices = (
	ngrams
		.rdd
		.zipWithIndex()
		.map(lambda n: ngram_index_row(n[0][0], n[1]))
		.toDF()
)

subreddit_vector_row = Row('subreddit', 'index', 'value')

subreddit_ngram_index = (
	subreddit_ngrams_exploded
		.join(ngram_indices, subreddit_ngrams_exploded.ngram == ngram_indices.ngram)
		.select('subreddit', 'index')
		.map(lambda p: subreddit_vector_row(p[0], (p[1], 1)))
)

subreddit_ngram_indices = (
	subreddit_ngram_index
		.groupByKey()
)

size = ngram_indices.count()

def to_sparse_vector(t):
	return SparseVector(size, t)

subreddit_vectors = (
	subreddit_ngram_indices
		.map(lambda n: Row(subreddit=n[0], vector=to_sparse_vector(n[1])))
)

subreddit_vectors.toDF().show()
