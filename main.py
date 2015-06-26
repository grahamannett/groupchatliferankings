import sqlite3
import pandas as pd
import nltk
import re
import twitter

from twitterutil import *

# start process_tweet


# most_recent_date = messages.
con = sqlite3.connect('chat.db')

messages = pd.read_sql_query(
    """select text,handle_id,id,date
    from message join handle on message.handle_id=handle.rowid
    where cache_roomnames="###" and
    item_type=0 and length(text)>1
    order by date desc""", con)

names = pd.read_csv('names.csv', index_col=0, dtype={'phone': str})

df = messages.merge(names, how='left', left_on='id', right_on='phone')
df.text = df.text.str.lower()
# get rid of hashtags
df.text = df.text.map(lambda x: re.sub(r'#([^\s]+)', r'\1', x))
# get rid of urls, just leave url
df.text = df.text.map(
    lambda x: re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', x))


df.blob = df.text.map(lambda x: TextBlob(str(x)))
df.rating = df.text.map(lambda x: TextBlob(str(x)).sentiment.polarity)

new_rankings = []
for x in df.name.value_counts().index:

    new_rankings.append('name: ' + str(x) + '; life rank:' + str((df[df.name == x].rating.sum() / df[df.name == x].shape[0]))[:5])
new_rankings = '\n'.join(new_rankings)


auth = twitter.OAuth(AccessToken, AccessSecret, ConsumerKey, ConsumerSecret)
# t = twitter.Twitter(auth=auth).account.verify_credentials()


t = twitter.Twitter(auth=auth).statuses.update(status=new_rankings)

# ordered list best to worst
for x in df.name.value_counts().index:

    new_rankings.append('name: ' + str(x) + '; life rank:' + str((df[df.name == x].rating.sum() / df[df.name == x].shape[0]))[:5])
new_rankings = '\n'.join(new_rankings)


