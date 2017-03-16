from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.mllib.linalg import SparseVector
from pyspark.sql.functions import col, explode

# ascii values of characters
ord_A = ord('A')
ord_Z = ord('Z')
ord_a = ord('a')
ord_z = ord('z')
ord_0 = ord('0')
ord_9 = ord('9')
ord_sp = ord(' ')

''' returns true if the character is 'allowed'
this will return true for all [a-zA-Z0-9], false otherwise
'''
def allowed_ord(o):
    return (
        (o>=ord_A and o<=ord_Z) or
        (o>=ord_a and o<=ord_z) or
        (o>=ord_0 and o<=ord_9)
    )

''' returns a list of tokens when given a string

'Hello world Foo? Bar!!!abc.d' -> ['hello', 'world', 'foo', 'bar', 'abc', 'd']

'''
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


''' given an 'n' (n-gram size) and a list of tokens l, returns the
list of ngrams, tokens delimited by space:
['hello', 'world', 'foo', 'bar', 'abc', 'd'] -> n=2
['hello world', 'world foo', 'foo bar', 'bar abc', 'abc d']
'''
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

'''
You dont need to run the stuff below here. You can jump straight to loading
the subreddit_ngram_indices / subreddit_user_indices
'''
df = sqlContext.read.json('hdfs:///user/lhoward/posts_2016_filtered.json')

''' converts a dataframe with all of the post information into a subreddit id
and the list of tokens in the post:

Row(subreddit='funny', title='Foo bar baz!', author='bob', num_comment=123, ...)
->
Row(subreddit='funny', tokens=['foo', 'bar', 'baz'])
'''
subreddit_tokens = (
    df
        .select('subreddit', 'title')
        .map(lambda p: Row(subreddit=p[0], tokens=tokenizer(p[1])))
        .toDF()
)

''' converts a dataframe with tokens into ngrams:
Row(subreddit='funny', tokens=['foo', 'bar', 'baz'])
->
Row(subreddit='funny', ngrams=['foo bar', 'bar baz'])
'''
subreddit_ngrams = (
    subreddit_tokens
        .select('subreddit', 'tokens')
        .map(lambda p: Row(subreddit=p[0], ngrams=ntoken(2, p[1])))
        .toDF()
)

''' basically selects just the subreddit and user id:
Row(subreddit='funny', title='Foo bar baz!', author='bob', num_comment=123, ...)
->
Row(subreddit='funny', user='bob')
'''
subreddit_users = (
    df
        .select(
            'subreddit',
            df.author.alias('user')
        )
)

''' expands (or in pyspark 'explodes') the ngrams
Row(subreddit='funny', ngrams=['foo bar', 'bar baz'])
->
Row('subreddit='funny', ngram='foo bar')
Row('subreddit='funny', ngram='bar baz')
'''
subreddit_ngrams_exploded = (
    subreddit_ngrams
        .select(
            'subreddit',
            explode('ngrams').alias('ngram')
        )
        .where(col('ngram').isNotNull())
        .distinct()
)

'''
Row('subreddit='funny', ngram='foo bar')
Row('subreddit='funny', ngram='bar baz')
Row('subreddit='other', ngram='foo bar')
->
Row(ngram='foo bar')
Row(ngram='bar baz')
'''
ngrams = (
    subreddit_ngrams_exploded
        .select('ngram')
        .distinct()
)

'''
Row('subreddit='funny', user='bob')
Row('subreddit='funny', user='fred')
Row('subreddit='other', user='bob')
->
Row(user='bob')
Row(user='fred')
'''
users = (
    subreddit_users
        .select('user')
        .distinct()
)

ngram_index_row = Row('ngram', 'index')
user_index_row = Row('user', 'index')

'''
Row(ngram='foo bar')
Row(ngram='bar baz')
->
Row(ngram='foo bar', index=0)
Row(ngram='bar baz', index=1)
'''
ngram_indices = (
    ngrams
        .rdd
        .zipWithIndex()
        .map(lambda n: ngram_index_row(n[0][0], n[1]))
        .toDF()
)

'''
Row(user='bob')
Row(user='fred')
->
Row(user='bob', index=0)
Row(user='fred', index=1)
'''
user_indices = (
    users
        .rdd
        .zipWithIndex()
        .map(lambda u: user_index_row(u[0][0], u[1]))
        .toDF()
)

'''
converts subreddit ngrams into vector components (index is the dimension)
Row('subreddit='funny', ngram='foo bar')
Row('subreddit='funny', ngram='bar baz')
->
Row('subreddit='funny', index=0)
Row('subreddit='funny', index=1)
'''
subreddit_ngram_indices = (
    subreddit_ngrams_exploded
        .join(ngram_indices, subreddit_ngrams_exploded.ngram == ngram_indices.ngram)
        .select('subreddit', 'index')
)

'''
converts subreddit users into vector components (index is the dimension)
Row(subreddit='funny', user='bob')
->
Row(subreddit='funny', index=0)
'''
subreddit_user_indices = (
    subreddit_users
        .join(user_indices, subreddit_users.user == user_indices.user)
        .select('subreddit', 'index')
)

# subreddit_ngram_indices.write.json('hdfs:///user/lhoward/posts_ngrams.json')
subreddit_ngram_indices = sqlContext.read.json('hdfs:///user/lhoward/posts_ngrams.json').select('subreddit', 'index')
# subreddit_user_indices.write.json('hdfs:///user/lhoward/posts_users.json')
subreddit_user_indices = sqlContext.read.json('hdfs:///user/lhoward/posts_users.json').select('subreddit', 'index')

#jaccard
def ngram_similarity(a,b):
    a_rdd = subreddit_ngram_indices.filter(subreddit_ngram_indices.subreddit==a).select('index').rdd
    b_rdd = subreddit_ngram_indices.filter(subreddit_ngram_indices.subreddit==b).select('index').rdd
    union = a_rdd.union(b_rdd).distinct().count()
    intersection = float(a_rdd.intersection(b_rdd).count())
    return intersection/union

def user_similarity(a,b):
    a_rdd = subreddit_user_indices.filter(subreddit_user_indices.subreddit==a).select('index').rdd
    b_rdd = subreddit_user_indices.filter(subreddit_user_indices.subreddit==b).select('index').rdd
    union = a_rdd.union(b_rdd).distinct().count()
    intersection = float(a_rdd.intersection(b_rdd).count())
    return intersection/union


from itertools import combinations
subreddits = ['politics', 'worldnews', 'Cooking', 'food', 'gaming', 'funny', 'videos']

for a,b in combinations(subreddits, 2):
    print '=' * 10
    print 'Comparing %s with %s' % (a,b)
    print '2-gram: %f' % ngram_similarity(a,b)
    print 'user: %f' % user_similarity(a,b)
