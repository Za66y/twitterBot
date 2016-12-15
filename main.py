from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, API
from tweepy import Stream
import json
import logging
import warnings
import time
from pprint import pprint
 
warnings.filterwarnings("ignore")
 
access_token = "**********"
access_token_secret = "**********"
consumer_key = "**********"
consumer_secret = "**********"
 
auth_handler = OAuthHandler(consumer_key, consumer_secret)
auth_handler.set_access_token(access_token, access_token_secret)
 
twitter_client = API(auth_handler)
 
logging.getLogger("main").setLevel(logging.INFO)
 
AVOID = []
 
 
class PyStreamListener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        self.saveFile = open('tweets.json', 'a')
        super(StreamListener, self).__init__()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            tweet = json.loads(data)
            self.saveFile.write(data)
            self.saveFile.write('\n')
            try:
                publish = True
                for word in AVOID:
                    if word in tweet['text'].lower():
                        logging.info("SKIPPED FOR {}".format(word))
                        publish = False

                if tweet.get('lang') and tweet.get('lang') != 'en':
                    publish = False

                if publish:
                    twitter_client.retweet(tweet['id'])
                    logging.debug("RT: {}".format(tweet['text']))

            except Exception as ex:
                logging.error(ex)

            return True
        else:
            self.saveFile.close()
            return False
 
    def on_error(self, status):
        print(status)
        if status == 420:
            return False

#time limit

 
if __name__ == '__main__':
    listener = PyStreamListener(time_limit=20)
    stream = Stream(auth_handler, listener)
    stream.filter(track=['#BigData', 'IoT', 'Java', 'JavaEE'], async=True)
