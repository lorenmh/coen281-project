# Overview

This project attempts to create a similarity metric by aggregating three types of similarity: User similarity, Text Similarity, and Meta Similarity.

# Running Scripts

In order to properly aggregate data, there are two different types of data sources to pull from. The first is a BigQuery and the other is the Reddit API. The BigQuery data is used in the User and Text Similarity. The Reddit API is used for meta similarity.

### User and Text Similarity

Once you've ensured that you have access to the BigQuery file, you need to run the individual scripts text_similarity.py and user_similarity.py. This will also show the output of the similarity

### Meta Similarity

This can be done without any Big Data tools although it may be wise to use a good machine or at least break up the jobs in case Reddit throttles you.

1. Install necessary python libraries: bs4, requests, praw
2. Get reddit API key and create a creds.json file in the metasimilarity folder.
  a. The JSON should be one anonymous object with the keys: 'id', 'secret', 'user-agent'
3. Once this is all set up, just run the main script. This should generate a dataset.csv file
4. To calculate the similarity, run meta_similarity.py

# Getting Similarities

Each similarity metric will have a different json format. Once all files have been generated, put them in the results folder. Run the aggregate.py script and it'll spit out the results.json file which contains the results of the similarity metric.
