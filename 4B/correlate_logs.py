#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  5 15:45:58 2017

@author: kartiw
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import operator
import sys
import uuid
from datetime import datetime
#import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages anguenot:pyspark-cassandra:0.6.0"

#import pyspark_cassandra

keyspace = sys.argv[1]
tablename = sys.argv[2]
cluster_seeds = ['199.60.17.171', '199.60.17.188']

#On Server
import pyspark_cassandra
conf = SparkConf().setAppName('load_logs_spark').set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)


def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size, row_format=pyspark_cassandra.RowFormat.DICT).setName(tablename)
    return rdd

def make_tuple(line):
    return (line['host'],(1,line['bytes']))

def add_pairs(kv0,kv1):
    tbytes=count=0
    tbytes=kv0[1]+kv1[1]
    count=kv0[0]+kv1[0]
    return count,tbytes

get_data=rdd_for(keyspace,tablename)

get_data = get_data.map(make_tuple)

reducedkeyrdd = get_data.reduceByKey(add_pairs)
reducedkeyrdd=reducedkeyrdd.cache()

sum_n =  reducedkeyrdd.count()

sum_x = reducedkeyrdd.map(lambda x: x[1][0]).reduce(operator.add)
sum_y = reducedkeyrdd.map(lambda x: x[1][1]).reduce(operator.add)
sum_xy = reducedkeyrdd.map(lambda x: x[1][0]*x[1][1]).reduce(operator.add)
sum_x2 = reducedkeyrdd.map(lambda x: x[1][0]**2).reduce(operator.add)
sum_y2 = reducedkeyrdd.map(lambda x: x[1][1]**2).reduce(operator.add)


r=((sum_n*sum_xy) - (sum_x*sum_y)) / (((sum_n*sum_x2 - sum_x**2)**0.5) * ((sum_n*sum_y2 - sum_y**2)**0.5))
r2=r**2

newrdd = sc.parallelize([r,r2])
print("r=",r," r^2",r2)
#newrdd.saveAsTextFile(output)
