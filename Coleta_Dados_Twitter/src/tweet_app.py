import tweepy
import json
import pandas as pd
import numpy as np
import pymongo

KEYS = {
    'consumer_key': "cole sua chave aqui"",
    'consumer_secret': "cole sua chave aqui"",
    'access_token': "cole sua chave aqui"",
    'access_token_secret': "cole sua chave aqui""
}

auth = tweepy.OAuthHandler(KEYS['consumer_key'], KEYS['consumer_secret'])
auth.set_access_token(KEYS['access_token'], KEYS['access_token_secret'])

api = tweepy.API(auth)

# PESQUISAR POR PALAVRA CHAcdVE
keyword = ('genocida' or 'AIDS' or 'bozo' or 'energumeno' or 'vacina')
tweets = []
info = []

# Contruir  instancia da API
token = tweepy.API(auth, wait_on_rate_limit=True)

for tweet in tweepy.Cursor(token.search_tweets,
                           q=keyword,
                           tweet_mode='extended',
                           count=200,
                           result_type="mixed",
                           lang='pt',
                           include_entities=True).items(200):
    info.append(tweet)

#Criando um arquivo txt
with open('tweets_keywords_genocida.txt', "w") as filename:
    for tweet in info:
        if 'retweeted_status' in dir(tweet):
            aux = tweet.retweeted_status.full_text
        else:
            aux = tweet.full_text

        newtweet = aux.replace("\n", " ")

        tweets.append(newtweet)

        file = open("tweets_keywords_genocida.txt", "a", -1, "utf-8")
        file.write(newtweet + '\n')
        file.close()
################Fim arquivo txt ##################

#Criando um arquivo csv
tweets_df = pd.DataFrame(tweets, columns=['Tweets'])

tweets_df['len'] = np.array([len(tweet) for tweet in tweets])
tweets_df['ID'] = np.array([tweet.id for tweet in info])
tweets_df['USER'] = np.array([tweet.user.screen_name for tweet in info])
tweets_df['username'] = np.array([tweet.user.name for tweet in info])
tweets_df['User Location'] = np.array([tweet.user.location for tweet in info])
tweets_df['Language'] = np.array([tweet.user.lang for tweet in info])
tweets_df['Date'] = np.array([tweet.created_at for tweet in info])
tweets_df['Source'] = np.array([tweet.source for tweet in info])
tweets_df['Likes'] = np.array([tweet.favorite_count for tweet in info])
tweets_df['Retweets'] = np.array([tweet.retweet_count for tweet in info])

tweets_df.to_csv('tweets_keywords_genocida.csv')
##################Fim arquivo csv#################

#Criando um arquivo json

with open('tweets_keywords_genocida.json', "w") as filename:
    for tweet in info:
        status = tweet

        json_str = json.dumps(status._json)

        parsed = json.loads(json_str)
        filename.write(json.dumps(parsed, indent=4))

filename.close()
#############Fim arquivo json###################

#Gravando no mongo
con = pymongo.MongoClient('localhost', 27017)

#Cria a database
db = con["db_tweets"]

#Criação da coleção
colecao = db["tweets_genocida"]

for tweet in info:
    json_str = json.dumps(tweet._json)

    parsed = json.loads(json_str)

    colecao.insert_one(parsed)
############Fim mongo####################
