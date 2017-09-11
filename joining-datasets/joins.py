#
# Source code for the 'Joining Datasets in Spark' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
# Execute this program using spark-submit as follows:
#
# 	$ spark-submit --master local joins.py \
#		$SPARK_HOME/data/bike-share \
#		$SPARK_HOME/data/avgsbystation
#

import sys, re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Joining Datasets in Spark')
sc = SparkContext(conf=conf)

# check command line arguments
if (len(sys.argv) != 3):
	print("""\
This program will find the top stations for average bikes available 
by station and hour from the Bay Area Bike Share dataset

Usage:  joins.py <data_dir> <output_dir>
""")
	sys.exit(0)
else:
	inputpath = sys.argv[1]
	outputdir = sys.argv[2]

stations = sc.textFile(inputpath + "/stations") \
	.map(lambda x: x.split(',')) \
	.filter(lambda x: x[5] == 'San Jose') \
	.map(lambda x: (int(x[0]), x[1])) \
	.keyBy(lambda x: x[0])

status = sc.textFile(inputpath + "/status")	\
	.map(lambda x: x.split(',')) \
	.map(lambda x: (x[0], x[1], x[2], x[3].replace('"',''))) \
	.map(lambda x: (x[0], x[1], x[2], x[3].split(' '))) \
	.map(lambda x: (x[0], x[1], x[2], x[3][0].split('-'), x[3][1].split(':'))) \
	.map(lambda x: (int(x[0]), int(x[1]), int(x[3][0]), int(x[3][1]), int(x[3][2]), int(x[4][0]))) \
	.filter(lambda x: x[2]==2015 and x[3]==2 and x[4]>=22) \
	.map(lambda x: (x[0], x[1], x[5])) \
	.keyBy(lambda x: x[0])

joined = status.join(stations)

cleaned = joined.map(lambda x: (x[0], x[1][0][1], x[1][0][2], x[1][1][1]))

topavail = cleaned.keyBy(lambda x: (x[3],x[2])) \
	.mapValues(lambda x: (x[1], 1)) \
	.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
	.mapValues(lambda x: (x[0]/x[1])) \
	.keyBy(lambda x: x[1]) \
	.sortByKey(ascending=False) \
	.map(lambda x: (x[1][0][0], x[1][0][1], x[0])) \
	.persist()
	
topavail.saveAsTextFile("file://" + outputdir)
top10stations = topavail.take(10)
print("The top ten stations by hour are : ")
for stationinfo in top10stations:
	print(str(stationinfo))
print("Check the complete output in " + outputdir)