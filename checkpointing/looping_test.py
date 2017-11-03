#
# Source code for the 'Checkpointing RDDs' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
# 	$ spark-submit --master looping_test.py
#

import sys
from pyspark import SparkConf, SparkContext
sc = SparkContext()
sc.setCheckpointDir("file:///tmp/checkpointdir")
rddofints = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
try:
	# this will create a very long lineage for rddofints
	for i in range(1000):
		rddofints = rddofints.map(lambda x: x+1)
		if i % 10 == 0:
			print("Looped " + str(i) + " times")
			#rddofints.checkpoint()
			rddofints.count()
except Exception as e:
	print("Exception : " + str(e))
	print("RDD Debug String : ")
	print(rddofints.toDebugString())
	sys.exit()
print("RDD Debug String : ")
print(rddofints.toDebugString()) 

