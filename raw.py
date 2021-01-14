import tweepy
import json
import argparse

from future.backports.datetime import time
from pymongo import MongoClient
from urllib3.exceptions import ProtocolError

# Twitter developer account authentication
CONSUMER_KEY = "XA4olFwtO3ID5GLCy16zbxPZ4"
CONSUMER_SECRET = "rj7788EZsBRVePC2yqBdsOrwf9GgyRxFxHU3kHuflBE3K7AZKG"
ACCESS_TOKEN = "1070251789788700672-qow9zZ99D62kgL44QSlewgf4H74NDv"
ACCESS_TOKEN_SECRET = "bYYvgeqfH9JFzOmOO7jlDf4lwFU12mtHwy20jXewBh4FN"

MONGO_HOST = "mongodb+srv://user:B00838586@cluster0.pfgdz.mongodb.net/?retryWrites=true&w=majority"

# WORDS = ["Storm", "Winter", "Canada", "Temperature", "Flu", "Snow", "Indoor", "Safety"]

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--keyword', help='<Required> A keyword given to filter the Twitter stream, eg. -wCanada',
                    required=True)
args = parser.parse_args()
KEYWORD = args.keyword


# Extracting date using Twitter Streaming API
class StreamListener(tweepy.StreamListener):

    def __init__(self, stream_listener):
        self.api = stream_listener
        super(StreamListener, self).__init__()
        self.num_tweets = 0

    def on_connect(self):
        print("Successfully connected to the streaming API.")

    def on_error(self, status_code):
        print('An Error has occurred: ' + repr(status_code))
        return False

    def on_limit(self, status):
        print("Rate Limit Exceeded, Sleep for 15 Mins")
        time.sleep(15 * 60)
        return True

    def on_data(self, data):

        try:
            client = MongoClient(MONGO_HOST)
            db = client.RawDb
            datajson = json.loads(data)
            self.num_tweets += 1
            if self.num_tweets < 200:
                id_str = datajson['id_str']
                db[KEYWORD].insert_one(datajson)
                print("Tweet with id: " + id_str + " saved in MongoDB.")
                return True
            else:
                return False
        except ProtocolError:
            print("")
        except Exception as e:
            print(e)


# Extracting date using the Search API
def save_search_mongodb():
    try:
        api = tweepy.API(auth)
        client = MongoClient(MONGO_HOST)
        db = client.RawDb
        collection = db[KEYWORD]
        for tweet in tweepy.Cursor(api.search, q=KEYWORD, lang='en').items(200):
            data = tweet._json
            id_str = data['id_str']
            collection.insert_one(data)
            print("Tweet with id: " + id_str + " saved in MongoDB.")
    except Exception as e:
        print(e)


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

listener = StreamListener(stream_listener=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)

print("Searching by keyword: " + KEYWORD)

save_search_mongodb()

print("Streaming by tracking keyword: " + KEYWORD)

while True:
    try:
        streamer.filter(track=[KEYWORD], languages=["en"], stall_warnings=True)
    except ProtocolError:
        continue
