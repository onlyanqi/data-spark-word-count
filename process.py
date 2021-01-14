from pyspark.shell import spark
from pyspark.sql.functions import regexp_replace, trim, col, lower, split, explode

WORDS = ["storm", "winter", "canada", "hot", "cold", "flu", "snow", "indoor", "safety", "rain", "ice"]


def clean_punctuation(column):
    return trim(lower(regexp_replace(column, '([^\s\w_]|_)+', ''))).alias('sentence')


df1 = spark.read.option("multiline", "true").json("cleanTweets/*.json")
df2 = spark.read.option("multiline", "true").json("reuters/*.json")
df1.printSchema()
df2.printSchema()

result = []


def clean_dataframe(df, column_name):
    textDF = df.select(clean_punctuation(col(column_name)))
    splitDF = (textDF.select(split(textDF.sentence, '\s+').alias('split')))
    singleDF = (splitDF.select(explode(splitDF.split).alias('word')))
    wordsDF = singleDF.where(singleDF.word != "")
    for row in wordsDF.rdd.collect():
        text = row[0]
        for w in WORDS:
            if w in text:
                count = text.count(w)
                for i in range(count):
                    result.append(w)


clean_dataframe(df1, 'text')
clean_dataframe(df2, 'reuters')


def frequency(mylist):
    # Creating an empty dictionary
    freq = {}
    for item in mylist:
        if item in freq:
            freq[item] += 1
        else:
            freq[item] = 1
    with open("word_frequency.txt", "w") as my_output_file:
        for key, value in freq.items():
            print("% s : % d" % (key, value))
            my_output_file.write("% s : % d \n" % (key, value))
    my_output_file.close()


frequency(result)
