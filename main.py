import sqlite3
import pandas as pd
import nltk
import re
from textblob import TextBlob
import itertools

# has my private keys and shit in it
import twitter
from twitterkeys import *

twitterauth = twitter.OAuth(AccessToken, AccessSecret, ConsumerKey, ConsumerSecret)
def post_to_twitter(message,myauth=twitterauth):
    "post message var to twitter with myauth"
    t = twitter.Twitter(auth=myauth).statuses.update(status=message)

def post_messagecount_twitter(df):
    """Prints the following:
    name: graham ; messages: 3360
    name: lex ; messages: 1733
    name: austin ; messages: 885
    name: con ; messages: 759
    """

    number_of_new_messages = []
    for x in df.name.value_counts().index:
        number_of_new_messages.append(' '.join(['name: ' + x, '; messages: '+ str(df[df.name == x].shape[0])]))
    number_of_new_messages = '\n'.join(number_of_new_messages)

    post_to_twitter(number_of_new_messages)
    # return True

def post_current_best(df):
    # post_curent= "Con's life is undeniably the best, not need to implement function\n" 
    
    post_to_twitter("con's life is best")


def post_life_score(df):
    """Prints the following:
    name: graham; life rank:0.010
    name: lex; life rank:0.014
    name: austin; life rank:0.086
    name: con; life rank:0.068
    """
    new_rankings = []
    for x in df.name.value_counts().index:

        new_rankings.append('name: ' + str(x) + '; life score:' + str((df[df.name == x].rating.sum() / df[df.name == x].shape[0]))[:5])
    new_rankings = '\n'.join(new_rankings)
    post_to_twitter(new_rankings)



def return_messages_df():
    con = sqlite3.connect('chat.db')

    messages = pd.read_sql_query(
        """select text,handle_id,id,date
        from message join handle on message.handle_id=handle.rowid
        where cache_roomnames="chat642480724793456515" and
        item_type=0 and length(text)>1
        order by date desc""", con)

    # latest date in sql table
    most_recent_date = messages.date[0]

    # write latest date to keep for next time

    last_date = int(open('latest_date.txt').read())
    print('last date recorded: ', last_date)
    print('most recent date', most_recent_date)
    if most_recent_date >= last_date:
        messages = messages[messages.date>last_date]
        with open('latest_date.txt','w') as textf: textf.write(str(most_recent_date))

    print('\n current length of df: ',messages.shape[0])
    # names associated with sql data phone numbers which I want to keep private
    names = pd.read_csv('names.csv', index_col=0, dtype={'phone': str})

    df = messages.merge(names, how='left', left_on='id', right_on='phone')


    # merge to have names with each
    df.text = df.text.str.lower()
    # get rid of hashtags
    df.text = df.text.map(lambda x: re.sub(r'#([^\s]+)', r'\1', x))
    # get rid of urls, just leave url
    df.text = df.text.map(lambda x: re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', x))


    return df


df = return_messages_df()

# series_to_convert = df['text']
# df['text'] =  [s.encode('ascii', 'ignore').strip() for s in series_to_convert.decode('unicode_escape')]

df['blob'] = df.text.map(lambda x: TextBlob(str(x)))
df['rating'] = df.text.map(lambda x: TextBlob(str(x)).sentiment.polarity)

#POSTING TO TWITTER BELO
post_life_score(df)
post_messagecount_twitter(df)
#####
#




# t = twitter.Twitter(auth=auth).account.verify_credentials()

#######
# number of messages for the past 3 days or whatever






ordering = []

for x in df.name.value_counts().index:

    ordering.append([x, str((df[df.name == x].rating.sum() / df[df.name == x].shape[0]))])


ordering.sort(key=lambda x: x[1],reverse=True)

ordering = list(itertools.chain(*ordering))



#####
