import credentials
import logging
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
from pymongo import MongoClient


def authenticate():
    """Function for handling Twitter Authentication. Please note
       that this script assumes you have a file called credentials.py
       which stores the 4 required authentication tokens:

       1. API_KEY
       2. API_SECRET
       3. ACCESS_TOKEN
       4. ACCESS_TOKEN_SECRET

    See course material for instructions on getting your own Twitter credentials.
    """
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    return auth


class MaxTweetsListener(StreamListener):

    def __init__(self, max_tweets, *args, **kwargs):
        # initialize the StreamListener
        super().__init__(*args, **kwargs)
        # set the instance attributes
        self.max_tweets = max_tweets
        self.counter = 0
        # instanciate mongo database
        self.client = MongoClient('mongodb')
        self.db = self.client['twitter']
        if self.db.list_collection_names() == []:
            self.db.create_collection('tweets')
        # make sure this works
        logging.critical('... created with mongodb connection')


    def on_connect(self):
        logging.critical(f'... listening for incoming {self.max_tweets} tweets:')


    def on_status(self, status):
        # extract information from status
        tweet = {
            'text': status.text,
            'username': status.user.screen_name,
            'followers_count': status.user.followers_count
        }

        # decide what to do here:
        # self.print_tweet(tweet)
        self.add_to_db(tweet)

        # keep track of the counter, eventually terminate
        self.counter += 1
        if self.max_tweets == self.counter:
            self.counter = 0
            logging.critical('... all tweets collected, listener end')
            return False


    def on_error(self, status):
        if status == 420:
            logging.critical('... Rate limit applies. Stop the stream.')
            return False


    def print_tweet(self, tweet):
        print(f'({self.counter}) {tweet["username"]}: {tweet["text"]}')


    def add_to_db(self, tweet):
        self.db.tweets.insert_one(tweet)
        logging.critical(f'{self.counter} tweet -> mongodb')


if __name__ == '__main__':
    auth = authenticate()
    listener = MaxTweetsListener(max_tweets=1000)
    stream = Stream(auth, listener)
    stream.filter(track=['ai'], languages=['en'], is_async=False)