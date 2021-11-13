'''
1. EXTRACT the tweets from mongodb
- connect to the database 
- query the data

2. TRANSFORM the data
- clean the text before?
- sentiment analysis
- maybe transform data types?

3. LOAD the data into postgres
- connect to postgres 
- insert into postgres

'''
import time
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from sqlalchemy import create_engine
import psycopg2
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


### create connections to databases
def connect(mytopic):

    # connect to mongodb
    modb = MongoClient('mongodb')['twitter']

    # connect to postgresdb
    pgdb_conn_string = f'postgresql://postgres:1234@postgresdb:5432/postgres'
    pgdb = create_engine(pgdb_conn_string)

    # drop/create table for given topic
    #pgdb.execute("DROP TABLE {mytopic}")
    query_head = f'CREATE TABLE IF NOT EXISTS {mytopic}'
    query_body = '''(
        mongoid VARCHAR(24) PRIMARY KEY,
        username VARCHAR(255),
        followers INT,
        sentiment FLOAT,
        tweettext TEXT
        );'''
    pgdb.execute(query_head + query_body)

    # make sure this works
    logging.critical('... created with mongodb and postgresdb connection')
    return modb, pgdb


def get_max_mongoid_in(pgdb, mytopic):
    ''' Extract max mongoid (12-byte hex as str) for given postgres database table '''
    try:
        maxresult = pgdb.execute(f"SELECT max(mongoid) FROM {mytopic}")
        result_id = maxresult.fetchone()[0]
        if result_id is not None:
            logging.critical(f'found max id: {result_id}')
            return str(result_id)
    except:
        pass

    default_id = '000000000000000000000000'
    logging.critical(f'using default id: {default_id}')
    return default_id
    

def extract(modb, max_id_str):
    ''' Extracts tweets from mongodb '''
    mylist = list( modb.tweets.find({"_id": {"$gt": ObjectId(max_id_str)}}) )
    logging.critical(f'extracted {len(mylist)} tweets')
    return mylist

    #logging.critical(f'mongodb: {modb.tweets.count_documents({})} docs')
    #return list( modb.tweets.find(sort=[('_id', pymongo.DESCENDING)]).limit(n) )
    #return []


def transform_and_load(pgdb, sia, tweet_list, mytopic, max_id_str):
    ''' Transform data: clean text, gets sentiment analysis from text, formats date
    Load final data into postgres '''
    i = 0
    for tweet in tweet_list:
        # check mongoid
        mongoid = str(tweet['_id'])
        if (mongoid > max_id_str):
            # parse and analyze
            username = tweet['username']
            followers = tweet['followers_count']
            tweettext = tweet['text']
            sentiment = sia.polarity_scores(tweettext)['compound']

            # clean tweettext
            tweettext = tweettext.replace("'", "")

            try:
                # insert into postgres database
                insert_query = f"INSERT INTO {mytopic} VALUES ('{mongoid}', '{username}', '{followers}', '{sentiment}', '{tweettext}')"
                pgdb.execute(insert_query)
                max_id_str = mongoid
                i += 1
            except:
                logging.critical(f"{mongoid} INSERT FAILED")
                continue
            #logging.critical(f"{mongoid}, {username} ({followers}): ({sentiment}) {tweettext}")

    logging.critical(f'transformed: {i} tweets')
    return max_id_str


if __name__== "__main__":

    # define topic
    mytopic = 'ai'

    # initiate analysis
    sia = SentimentIntensityAnalyzer()

    # connect to both databases
    modb, pgdb = connect(mytopic)

    # intiate curent last id for the postgres database
    max_id_str = get_max_mongoid_in(pgdb, mytopic)

    # run ETL job in a fixed interval
    while True:
        tweet_list = extract(modb, max_id_str)
        max_id_str = transform_and_load(pgdb, sia, tweet_list, mytopic, max_id_str)
        time.sleep(15)

    

