#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import re, datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext


conf = SparkConf().setAppName('Ingest Logs')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
inputs = sys.argv[1]
output = sys.argv[2]
spark = SparkSession.builder.appName('example application').getOrCreate()

def words_once(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    line = line_re.match(line)
    if line:
        line = line.group(0)
        line = line.split()
        hostname = line[0]
        datestring = line[3][1:]
        bytes = line[-1]

        datestring = datetime.datetime.strptime(datestring, '%d/%b/%Y:%H:%M:%S')
        return (hostname, datestring, bytes)

text = sc.textFile(inputs)
words = text.map(words_once)
df = spark.createDataFrame(words)
df.write.format('parquet').save(output)
