#!/DCNFS/applications/cdh/5.6/app/spark-1.5.0-cdh5.6.0/bin/spark-submit

from pyspark import SparkContext, SparkConf
import itertools, math
from ast import literal_eval as make_tuple

conf = SparkConf()
conf.setAppName("Simple App")
sc = SparkContext(conf=conf)
# Suppress log warnings to errors only, 
# doesn't apply right away but does the trick
sc.setLogLevel("ERROR")

data = sc.textFile("hdfs:///user/adiaztos/subreddits_per_author.txt")
# data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
data = data.map(lambda x: make_tuple(x))
data = data.map(lambda x: map(lambda s: str(s), x[1]))

# print(data.take(5))

count = data.count()
support = count * 0.002
print(support)

#transactions = data.map(lambda line: line.strip().split(' '))
# transactions = sc.parallelize(data)
transactions = data

# print(transactions.collect())

setSize = 1
freq_item_set = {}

while setSize < 3:
	new_fis = {}
	# model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
	result = transactions.flatMap(lambda x: map(lambda y: (y, 1), itertools.combinations(x,setSize))).countByKey()
	# result = model.freqItemsets().collect()

	for fi in result:
		if result[fi] >= support:
			new_fis[fi] = result[fi]

	keys = [set(item) for item in new_fis]

	if len(new_fis) is 0:
		break
	else:
		# print(transactions.map(lambda t: list(set.union(*[x.intersection(set(t)) for x in keys]))).collect())
		# Filter out non frequent sets
		transactions = transactions.map(lambda t: list(set.union(*[x.intersection(set(t)) for x in keys])))
		freq_item_set.update({setSize: new_fis})

	setSize += 1

setSize = 2
sims = {}
# generate association rules for sets larger than 1
while setSize > 1:
	for fis in freq_item_set[setSize]:
		for item in itertools.combinations(fis, setSize-1):
			if (item in freq_item_set[setSize-1]):
				if (fis in sims):
					sims[fis] *= freq_item_set[setSize][fis]/float(freq_item_set[setSize-1][item])
				elif (fis[::-1] in sims):
					sims[fis[::-1]] *= freq_item_set[setSize][fis]/float(freq_item_set[setSize-1][item])
				else:
					sims[fis] = freq_item_set[setSize][fis]/float(freq_item_set[setSize-1][item])
	setSize -= 1

for pair in sims:
	print(pair, math.sqrt(sims[pair]))

sc.stop()
