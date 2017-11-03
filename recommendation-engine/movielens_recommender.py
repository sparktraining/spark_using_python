#
# Source code for the 'Implementing a Recommender Using Spark MLlib' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
# Execute this program using spark-submit as follows:
#
# 	$ spark-submit movielens_recommender.py \
#		hdfs:///path/to/movielens.dat
#

import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation \
import ALS, MatrixFactorizationModel, Rating
conf = SparkConf().setAppName('Movielens Recommender')
sc = SparkContext(conf=conf)

# check command line arguments
if (len(sys.argv) != 2):
	print("""\
This program will train and test a recommendation engine
using the movielens dataset

Usage:  movielens_recommender.py <data_dir>
""")
	sys.exit(0)
else:
	inputpath = sys.argv[1]

data = sc.textFile(inputpath) 
ratings = data.map(lambda x: x.split('\t')) \
    .map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))

rank = 10 
numIterations = 10 
model = ALS.train(ratings, rank, numIterations)

testdata = ratings.map(lambda p: (p[0], p[1])) 
predictions = model.predictAll(testdata) \
     .map(lambda r: ((r[0], r[1]), r[2])) 
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])) \
     .join(predictions) 
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2) \
     .mean() 
model.save(sc, "ratings_model")
print("Mean Squared Error = " + str(MSE)) 