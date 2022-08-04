from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from kafka import KafkaProducer
import requests
import json
import datetime
import socket

def process(rdd):
    if not rdd.isEmpty():
        data = rdd.collect()
        for i in data:
            print(i[0], i[1])
            requests.post('http://0.0.0.0:65432/', json={'hashtag':i[0],'count':i[1], 'timestamp':str(datetime.datetime.now())})


sc = SparkContext()
sc.setLogLevel('ERROR')
spark = SparkSession(sc)
sql_context = SQLContext(sc)
ssc = StreamingContext(sc, 20)
ssc.checkpoint("checkpoint_TwitterApp")
d_stream = ssc.socketTextStream("0.0.0.0", 3000)
d_streamWindow = d_stream.window(40,40)
words = d_streamWindow.flatMap(lambda x : x.split())
hashtags = words.filter(lambda w: ('#' in w and (w == '#ARSMUN' or w == '#MIvsLSG' or w == '#NBA' or w == '#WillSmith' or w == '#KGF2'))).map(lambda x: (x, 1))
tags_totals = hashtags.reduceByKey(lambda x, y : x+y)
tags_totals.foreachRDD(process)
ssc.start()
ssc.awaitTermination(3600)
ssc.stop()