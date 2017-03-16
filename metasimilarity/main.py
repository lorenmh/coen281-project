"""
This module does data aggregation and data formation for metasimilarity. It's written purely in
Python and there's no need for Spark.

It creates a couple of files such as affixes.txt, subreddits.txt, and mods.txt. In the end it creates a digestible csv file called dataset.csv

Required dependencies: requests, BeautifulSoup4, praw
"""

import csv
import json
import os
import re
import sys

import praw
import requests
from bs4 import BeautifulSoup
from bs4 import Comment
from bs4 import Tag

def popular_subreddits(num_pages):
    """
    Gets the first num_pages from redditlist.com and collects sfw subreddit names. Each page has
    150 subreddits.

    Positional Arguments:
        num_pages: number of pages from redditlist to scrape
    """

    subreddits = []

    with open('subreddits.txt', 'w') as f:
        for i in range(1,num_pages+1):
            listing = None
            page = requests.get('http://redditlist.com/?page=' + str(i))
            soup = BeautifulSoup(page.text, 'html.parser')
            nearby_tags = soup.find(string=lambda text:isinstance(text,Comment)
                                                        and 'Top Subscribers' in text).next_siblings

            while True:
                listing = next(nearby_tags)
                if listing == None:
                    break

                if isinstance(listing, Tag) and  listing['class'] == ['span4', 'listing']:
                    break

            for child in listing.contents:
                try:
                    if child.attrs['data-target-filter'] == 'sfw':
                        subreddits.append(child.attrs['data-target-subreddit'])
                except (KeyError, AttributeError):
                    continue
        f.write('\n'.join(subreddits))

def setup_praw():
    """Sets up praw and returns the Reddit instance."""
    config = json.load(open('creds.json', 'r'))
    return praw.Reddit(client_id=config['id'],
                         client_secret=config['secret'],
                         user_agent=config['user-agent'])

def get_popular_subreddits():
    """Returns the list of subreddits in the file subreddits.txt."""

    if not os.path.isfile('./subreddits.txt'):
        print ('Could not find subreddits.txt file for popular subreddits.', file=sys.stderr)

    with open('./subreddits.txt', 'r', newline='') as csvfile:
        return [subreddit[0] for subreddit in csv.reader(csvfile)]

def wiki_mentions(subreddit, reddit):
    """Goes through wiki pages and returns a list of mentioned subreddits."""
    srRegex = re.compile('(?:r\/).[^\(\)\]\.\s\:\+]*')
    mentions = set()
    try:
        for wikipage in reddit.subreddit(subreddit).wiki:
            for sr in srRegex.findall(wikipage.content_md):
                if subreddit not in sr and subreddit.lower() not in sr:
                    try:
                        second_slash = sr.index('/', 3, len(sr))
                    except:
                        second_slash = len(sr)
                    mentions.add(sr[2:second_slash])


        return '|'.join(list(mentions))
    except:
        return ''

def get_mod_list(reddit, subreddit):
    """Get a a list of moderators for the subreddit passed in."""
    return '|'.join([m.name for m in reddit.subreddit(subreddit).moderator()])

def lcs(string1, string2):
    """Naive lcs implementation, returning the common substring."""
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

def get_affixes(subreddits, support):
    """Returns the affixes present in the set of subreddit names given the support threshold."""
    first_round = set()
    for i in range(len(subreddits)):
        print(i)
        for j in range(len(subreddits)):
            if i == j:
                continue
            sequence = lcs(subreddits[i].lower(), subreddits[j].lower())

            if len(sequence) > 3:
                first_round.add(sequence)

    first_round = list(first_round)
    d = {first_round[i]: 0 for i in range(len(first_round))}
    final_cut = {}
    for i in range(len(first_round)):
        print(i)
        for j in range(len(first_round)):
            if i == j:
                continue
            sequence = lcs(first_round[i], first_round[j])
            if len(sequence) > 3 and sequence in d:
                d[sequence] += 1

    for k, v in d.items():
        if v > support:
            final_cut[k] = v

    with open('affixes.txt', 'w') as f:
        for k in final_cut.keys():
            f.write('{}\n'.format(k))

def sr_affixes(subreddit):
    """Return affixes for the given subreddit as a string."""
    sr_affixes = []
    with open('affixes.txt', 'r') as f:
        affixes = f.readlines()
        for line in affixes:
            if line in subreddit:
                sr_affixes.append(line)

    try:
        return ('|'.join(sr_affixes))
    except:
        return ''

# if you need to do data aggregation, then uncomment the following lines
"""
reddit = setup_praw()
popular_subreddits(8)           # top 1000
get_affi

"""

# actually combining different datasources into the format:
# name,affix1|affix2|affix3,mod1|mod2,mention1|mention2
with open('dataset.csv', 'w') as f:
    for subreddit in get_popular_subreddits():
        print (subreddit)
        f.write('{},{},{},{}\n'.format(subreddit, sr_affixes(subreddit),
                get_mod_list(reddit, subreddit).encode('ascii', 'ignore'), wiki_mentions(subreddit, reddit).encode('ascii', 'ignore')))
