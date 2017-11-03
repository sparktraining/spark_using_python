#
# Source code for the 'Using Broadcast Variables and Accumulators' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
# 	$ spark-submit --master local average_word_length.py
#

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Using Broadcast Variables and Accumulators')
sc = SparkContext(conf=conf)

# step 2
import urllib.request
stopwordsurl = "https://s3.amazonaws.com/sparkusingpython/stopwords/stop-word-list.csv"
req = urllib.request.Request(stopwordsurl)
with urllib.request.urlopen(req) as response:
	stopwordsdata = response.read().decode("utf-8") 
stopwordslist = stopwordsdata.split(",")
# step 3
stopwords = sc.broadcast(stopwordslist)
# step 4
word_count = sc.accumulator(0)
total_len = sc.accumulator(0.0)
# step 5
def add_values(word,word_count,total_len):
	word_count += 1
	total_len += len(word)
# step 6
words = sc.textFile('file:///opt/spark/data/shakespeare.txt') \
	.flatMap(lambda line: line.split()) \
	.map(lambda x: x.lower()) \
	.filter(lambda x: x not in stopwords.value)
# step 7
words.foreach(lambda x: add_values(x, word_count, total_len)) 
# step 8
avgwordlen = total_len.value/word_count.value
print("Total Number of Words: " + str(word_count.value))
print("Average Word Length: " + str(avgwordlen))