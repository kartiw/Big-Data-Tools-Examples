#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 12 16:43:28 2017

@author: kartiw
"""

"""
Sample Kafka consumer, to verify that messages are coming in on the topic we expect.
"""
import sys
from kafka import KafkaConsumer
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,functions


conf = SparkConf().setAppName('read_stream')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.appName('read_stream').getOrCreate()
 

topic = sys.argv[1]

#consumer = KafkaConsumer(topic, bootstrap_servers=['199.60.17.210', '199.60.17.193'])
#for msg in consumer:
 #   print(msg.value.decode('utf-8'))
    
messages = spark.readStream.format('kafka') \
       .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
messages.createOrReplaceTempView("messages")
messages = spark.sql("SELECT CAST(value as String) from messages")

split_col = functions.split(messages['value'], ' ')
messages = messages.withColumn("x", split_col.getItem(0))
messages = messages.withColumn("y", split_col.getItem(1))
messages.createOrReplaceTempView("messages")

#sum_xy=spark.sql("""SELECT SUM(x*y) FROM messages""").head()[0]
#sum_x=spark.sql("""SELECT SUM(x) FROM messages""").head()[0]
#sum_y=spark.sql("""SELECT SUM(y) FROM messages""").head()[0]
#sum_x2=spark.sql("""SELECT SUM(x*x) FROM messages""").head()[0]
#n=spark.sql("""SELECT COUNT(*) FROM messages""").head()[0] 

#b=(sum_xy - (sum_x*sum_y)/n) / (sum_x2 - (sum_x**2)/n)
#a=(sum_y/n) - b*(sum_x/n)

#rdd = sc.parallelize([b,a])
#df=rdd.toDF(["b","a"])

b=messages.selectExpr("SUM(x*y) - (SUM(x)*SUM(y))/COUNT(x) / SUM(x*x) - (SUM(x)*SUM(x))/COUNT(x) as b", "SUM(y)/COUNT(x) as term1", "SUM(x)/COUNT(x) as term2")
a=b.selectExpr("term1 - b*term2 as a", "b")


stream = a.writeStream.format('console') \
        .outputMode('update').start()
stream.awaitTermination(600)