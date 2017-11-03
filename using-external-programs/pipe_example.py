#
# Source code for the 'Processing RDDs with External Programs' Example in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
# Execute this program using spark-submit as follows:
#
# 	$ spark-submit --master local pipe_example.py
#

import sys
import os.path
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Using External Programs')
sc = SparkContext(conf=conf)

# check for parsefixedwidth.pl script
if os.path.isfile('parsefixedwidth.pl'):
	sc.addFile("parsefixedwidth.pl")
	fixed_width = sc.parallelize(['3840961028752220160317Hayward     CA94541'])
	piped = fixed_width.pipe("parsefixedwidth.pl") \
		.map(lambda x: x.split('\t'))
	print(piped.collect())	
else:
	print("""\
The parsefixedwidth.pl script must exist in the current directory
""")
	sys.exit(0)