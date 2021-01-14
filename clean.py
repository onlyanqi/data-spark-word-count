import re

from bson import json_util
from pymongo import MongoClient

MONGO_HOST = "mongodb+srv://user:B00838586@cluster0.pfgdz.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(MONGO_HOST)
db1 = client.RawDb
db2 = client.ProcessedDb


def clean_emoji(text):
    emoji_pattern = re.compile("(\\\\u)[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]")

    return emoji_pattern.sub(r'', text)


def clean_url(text):
    return re.sub(r'(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]', '', text)


collection = ''
while collection != 'q':
    collection = input("Enter the collection you want to clean in RawDb, enter 'q' to quit: ")
    myCollection1 = db1[collection]
    myCollection2 = db2[collection]
    for document in myCollection1.find():
        myResult = json_util.dumps(document)
        print(myResult)
        copy = myResult
        noemoji = clean_emoji(copy)
        nourl = clean_url(noemoji)
        print(nourl)
        data = json_util.loads(nourl)
        myCollection2.insert_one(data)
