#
# Source code for the 'Getting Started with Spark Streaming' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
#	in one terminal execute:
# 	$ spark-submit --master local[2] streaming_wordcount.py
#
#	in a second terminal execute:
#	$ sh stream_words.sh
#

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Getting Started with Spark Streaming')
sc = SparkContext(conf=conf)

import re 
from pyspark.streaming import StreamingContext 
ssc = StreamingContext(sc, 30) 
lines = ssc.socketTextStream('localhost', 9999) 
wordcounts = lines.filter(lambda line: len(line) > 0) \
              .flatMap(lambda line: re.split('\W+', line)) \
              .filter(lambda word: len(word) > 0) \
              .map(lambda word: (word.lower(), 1)) \
              .reduceByKey(lambda x, y: x + y) 
wordcounts.pprint() 
ssc.start() 
ssc.awaitTermination() 