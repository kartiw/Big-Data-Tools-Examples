#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 21:24:01 2017

@author: kartiw
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, SQLContext, Row
from pyspark.sql.functions import *

import sys
import datetime
import re

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('ingest logs')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.appName('example application').getOrCreate()


def words_once(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    splitted_line=line_re.split(line)
    #yield(len(splitted_line))
    if len(splitted_line) > 5:
        date = datetime.datetime.strptime(splitted_line[2], '%d/%b/%Y:%H:%M:%S')
        yield(splitted_line[1], date, splitted_line[3], float(splitted_line[4]))
        
text = sc.textFile(inputs)
words = text.flatMap(words_once)
#words.map(lambda x: Row(name=x[0], date=[1], add=[2],byte=x[3]))
df=spark.createDataFrame(words)
df.write.format('parquet').save(output)

#sqlContext = SQLContext(sc)
#newDataDF = sqlContext.read.parquet(output)


#newDataDF.createOrReplaceTempView('newDataDF')
#add = spark.sql("""SELECT SUM(_4) FROM newDataDF""")
#print(add.collect()[0][0])