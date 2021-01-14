import re

from pymongo import MongoClient

MONGO_HOST = "mongodb+srv://user:B00838586@cluster0.pfgdz.mongodb.net/?retryWrites=true&w=majority"

results = []

file1 = open("reut2-009.sgm", "r")
content1 = (file1.read())
file2 = open("reut2-014.sgm", "r")
content2 = (file2.read())

client = MongoClient(MONGO_HOST)
db = client.ReuterDb


def tag_extraction(collection, content):
    reuters_pattern = r"<REUTERS\b[^>]*>([\s\S]*?)(.*?)</REUTERS>"
    text_pattern = r"<TEXT\b[^>]*>([\s\S]*?)(.*?)</TEXT>"
    title_pattern = r"<TITLE>(.*?)</TITLE>"
    for reuter in re.findall(reuters_pattern, content):
        str_reuter = ''.join(reuter)
        temp = [str_reuter]
        for text in re.findall(text_pattern, str_reuter):
            str_text = ''.join(text)
            temp = [str_reuter, str_text, None]
            for title in re.findall(title_pattern, str_text):
                str_title = ''.join(title)
                temp = [str_reuter, str_text, str_title]
        results.append(temp)
    print(len(results))

    for i in range(len(results)):
        data = dict(article="article" + str(i + 1), reuters=results[i][0], text=results[i][1], title=results[i][2])
        db[collection].insert_one(data)


collection1 = "articles-reut2-009"
tag_extraction(collection1, content1)
print("Content in reut2-009 between 3 tags extracted and saved into 'articles-reut2-009' in ReuterDb")
results.clear()
collection2 = "articles-reut2-014"
tag_extraction(collection2, content2)
print("Content in reut2-014 between 3 tags extracted and saved into 'articles-reut2-014' in ReuterDb")
