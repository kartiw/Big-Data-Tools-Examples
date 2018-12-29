#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 17:59:26 2017

@author: kartiw
"""
from pyspark import SparkConf, SparkContext
import sys
import json

inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('reddit avg')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)
assert sys.version_info >= (3, 4)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def js(line):
    data=json.loads(line)
    yield(data["subreddit"], (data['score'], 1))
    
def get_key(kv):
    return kv[0]

def freq(kv):
    return kv[1]
 
def output_format(kv):
    k, v = kv
    return json.dumps((k,v))
    #return '%s %i' % (k, v)

def add_pairs(kv0,kv1):
    s=c=0 #s=score , c=counts
    s=kv0[0]+kv1[0]
    c=kv0[1]+kv1[1]
    return s,c
    
    
text = sc.textFile(inputs)
words = text.flatMap(js)
#wordcount=words
wordadd = words.reduceByKey(add_pairs)
avg=wordadd.mapValues(lambda x: float(x[0]) / x[1]).map(output_format)
avg.saveAsTextFile(output)

