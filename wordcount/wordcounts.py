#
# Source code for the 'MapReduce and Word Count' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
# Execute this program using spark-submit as follows:
#
# 	$ spark-submit --master local wordcounts.py \
#		$SPARK_HOME/data/shakespeare.txt \
#		$SPARK_HOME/data/wordcounts
#

import sys, re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Word Counts')
sc = SparkContext(conf=conf)

# check command line arguments
if (len(sys.argv) != 3):
	print("""\
This program will count occurances of each word in a document or documents
and return the counts sorted by the most frequently occuring words

Usage:  wordcounts.py <input_file_or_dir> <output_dir>
""")
	sys.exit(0)
else:
	inputpath = sys.argv[1]
	outputdir = sys.argv[2]

# count and sort word occurances	
wordcounts = sc.textFile("file://" + inputpath) \
			   .filter(lambda line: len(line) > 0) \
			   .flatMap(lambda line: re.split('\W+', line)) \
			   .filter(lambda word: len(word) > 0) \
			   .map(lambda word:(word.lower(),1)) \
			   .reduceByKey(lambda v1, v2: v1 + v2) \
			   .map(lambda x: (x[1],x[0])) \
			   .sortByKey(ascending=False) \
			   .persist()
wordcounts.saveAsTextFile("file://" + outputdir)
top5words = wordcounts.take(5)
justwords = []
for wordsandcounts in top5words:
	justwords.append(wordsandcounts[1])
print("The top five words are : " + str(justwords))
print("Check the complete output in " + outputdir)