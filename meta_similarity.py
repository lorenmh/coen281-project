import json
import requests

# used to check affixes are real words
DICTIONARY = requests.get('http://www.mieliestronk.com/corncob_lowercase.txt').text.split()

def main():
    dataset = parse_data('metasimilarity/dataset.csv', 10)
    similarities = []
    for sr in dataset:
        affix = affix_similarity(dataset, sr)
        wiki = wiki_similarity(dataset, sr)
        mod = mod_similarity(dataset, sr)

        for k in wiki.keys():
            similarities.append({
                'sr1': sr[0],
                'sr2': k,
                'score': affix[k] * .3 + wiki[k] * .6 + mod[k] * .1
            })

    json.dump(similarities, open('metasimilarity/simliarities.json', 'w'), sort_keys=True, indent=4)

def lcs(string1, string2):
    answer = ""
    len1, len2 = len(string1), len(string2)
    for i in range(len1):
        match = ''
        for j in range(len2):
            if i + j < len1 and string1[i + j] == string2[j]:
                match += string2[j]
            else:
                if (len(match) > len(answer)):
                    answer = match
                match = ''
        if len(match) > len(answer):
            answer = match

    try:
        return answer
    except:
        return ''

def parse_data(file_name, num_lines):
    """File is csv file of format subreddit name, affixes, mods, mentions."""
    subreddits = []
    with open(file_name, 'r') as f:
        for i in range(num_lines):
            subreddits.append(f.readline().split(','))

    return subreddits

def affix_similarity(dataset, targetsub):
    """Iterates through the list of affixes and returns a list of pairs ('subreddit', similar)."""
    similarities = {}
    affixlist = targetsub[1].split('|')

    for subreddit in dataset:
        if targetsub == subreddit:
            continue
        for affix in affixlist:
            if affix in subreddit[0] and len(affix) > 3:
                similarities[subreddit[0]] =  1
            else:
                # augment similarities if no affix found
                common = lcs(subreddit[0], targetsub[0])
                if common  in DICTIONARY:
                    print(common)
                    similarities[subreddit[0]] = 0.5
                else:
                    similarities[subreddit[0]] =  0

    return similarities


def mod_similarity(dataset, targetsub):
    similarities = {}
    modlist = targetsub[2].split('|')

    for subreddit in dataset:
        if targetsub == subreddit:
            continue
        num_similar = 0
        for mod in modlist:
            if mod == subreddit[2]:
                num_similar += 1
        similarities[subreddit[0]] = num_similar/len(modlist)
    return similarities

def wiki_similarity(dataset, targetsub):
    similarities = {}
    mentions = targetsub[3].split('|')

    for subreddit in dataset:
        if targetsub == subreddit:
            continue
        top_score = 0
        for mention in mentions:
            if mention.lower() == subreddit[0].lower():
                top_score = 1
            elif mention.lower() in [m.lower() for m in subreddit[3]]:
                top_score = 0.5

        similarities[subreddit[0]] = top_score
    return similarities



if __name__ == '__main__':
    main()
