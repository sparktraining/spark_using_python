#
# Source code for the 'Using Spark with Kafka' Exercise in
# Data Analytics with Spark Using Python
# by Jeffrey Aven
#
#	in one terminal, start Kafka
#
#	in a second terminal, create a topic named shakespeare
#	$ $KAFKA_HOME/bin/kafka-topics.sh \
#	--create \
#	--zookeeper localhost:2181 \
#	--replication-factor 1 \
#	--partitions 1 \
#	--topic shakespeare
#
#   in the same terminal execute:
# 	$ spark-submit --master local[2] \
#	--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
#	kafka_streaming_wordcount.py
#
#	in a third terminal execute:
#	$ sh stream_words.sh
#

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName('Using Spark with Kafka')
sc = SparkContext(conf=conf)
from pyspark.streaming import StreamingContext 
from pyspark.streaming.kafka import KafkaUtils 
ssc = StreamingContext(sc, 30) 
brokers = "localhost:9092" 
topic = "shakespeare" 
stream = KafkaUtils.createDirectStream \
(ssc, [topic], {"metadata.broker.list": brokers}) 
lines = stream.map(lambda x: x[1]) 
counts = lines.flatMap(lambda line: line.split(" ")) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a+b) 
counts.pprint() 
ssc.start() 
ssc.awaitTermination()