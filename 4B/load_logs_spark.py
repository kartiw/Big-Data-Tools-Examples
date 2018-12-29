#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  3 14:05:00 2017

@author: kartiw
"""
from pyspark import SparkConf, SparkContext
import sys
import re
import uuid
from datetime import datetime
#import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages anguenot:pyspark-cassandra:0.6.0"

#import pyspark_cassandra

inputs = sys.argv[1]
keyspace = sys.argv[2]
tablename = sys.argv[3]
cluster_seeds = ['199.60.17.171', '199.60.17.188']

#On Server
import pyspark_cassandra
conf = SparkConf().setAppName('load_logs_spark').set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
#spark = SparkSession.builder.getOrCreate()

#locally
#import pyspark_cassandra
#conf = SparkConf().setAppName('load_logs_spark').set("spark.cassandra.connection.host", "127.0.0.1")
#sc = pyspark_cassandra.CassandraSparkContext(conf=conf)


#conf = SparkConf().setAppName('ingest logs')
#sc = SparkContext.getOrCreate(conf=conf)

def words_once(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    splitted_line=line_re.split(line)
    #yield(len(splitted_line))
    if len(splitted_line) > 5:
        date = datetime.strptime(splitted_line[2], '%d/%b/%Y:%H:%M:%S')
        yield(splitted_line[1], date, splitted_line[3], float(splitted_line[4]))
        
def make_dictionary(line):
    row = {}
    row['host'] = line[0]
    row['datetime'] = line[1]
    row['path'] = line[2]
    row['bytes'] = line[3]
    row['identifier'] = str(uuid.uuid1())
    return row


text = sc.textFile(inputs)
words = text.flatMap(words_once)

rows_toDB = words.map(make_dictionary)
rows_toDB.saveToCassandra(keyspace, tablename)
