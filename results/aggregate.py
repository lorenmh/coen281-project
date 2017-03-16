import json

text = json.load(open('text_similarity.json'))
user = json.load(open('user_similarity.json'))
meta = json.load(open('meta_similarity.json'))

weights = {
    'ngram': .3,
    'user': .3,
    'author': .3,
    'meta': .1
}
subreddits = ['AskReddit', 'funny', 'todayilearned', 'science', 'worldnews', 'pics', 'IAmA', 'gaming', 'videos', 'movies']

similarities = []

for i in range(len(subreddits)):
    for j in range(i+1, len(subreddits)):
        text_sim = 0
        user_sim = 0
        meta_sim = 0

        # find each of the associated entries
        for el in text:
            if (el['s1'] == subreddits[i] and el['s2'] == subreddits[j] or
                el['s1'] == subreddits[j] and el['s2'] == subreddits[i]):
                    text_sim = el['ngram'] * weights['ngram'] + el['user'] * weights['user']
                    break

        for el in user:
            if (el['s1'] == subreddits[i] and el['s2'] == subreddits[j] or
                el['s1'] == subreddits[j] and el['s2'] == subreddits[i]):
                    user_sim = el['score'] * weights['author']

        for el in meta:
            if (el['s1'] == subreddits[i] and el['s2'] == subreddits[j] or
                el['s1'] == subreddits[j] and el['s2'] == subreddits[i]):
                    meta_sim = el['score'] * weights['meta']

        similarities.append({
            's1': subreddits[i],
            's2': subreddits[j],
            'text': text_sim,
            'user': user_sim,
            'meta': meta_sim,
            'total': text_sim + user_sim + meta_sim
        })

json.dump(similarities, open('results.json', 'w'), indent=4)
