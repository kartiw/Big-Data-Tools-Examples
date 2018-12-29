#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 17:59:26 2017

@author: kartiw
"""
from pyspark import SparkConf, SparkContext
from collections import defaultdict
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
    
def relative(line):
    #subred = line[0]
    comment = line[0][0]
    avg = line[0][1]
    return (comment['score']/ avg, comment['author']) 
 
def output_format(kv):
    k, v = kv
    return json.dumps((k,v))
    #return '%s %i' % (k, v)
    
def check_negative(cell):
    if cell[1] > 0:
        return cell
    
def comments_extract(line):
    data=json.loads(line)
    return data

def add_pairs(kv0,kv1):
    s=c=0 #s=score , c=counts
    s=kv0[0]+kv1[0]
    c=kv0[1]+kv1[1]
    return s,c
    
def get_key(kv):
    return kv[0]

    
text = sc.textFile(inputs)
words = text.flatMap(js)

wordadd = words.reduceByKey(add_pairs)
avg=wordadd.mapValues(lambda x: float(x[0]) / x[1])

avg=avg.map(check_negative)
avg_dict = dict(avg.collect()) #for reddit-1 3 records were collected

commentbyreddit = text.map(comments_extract)

bcast=sc.broadcast(avg_dict)

joinedrdd=commentbyreddit.map(lambda x : (x, bcast.value.get(x['subreddit'])))
joinedrdd.cache()


relativescore = joinedrdd.map(lambda x: (x[0]['score'] / x[1], x[0]['author']))
relativescore = relativescore.sortBy(get_key, False)

relativescore.saveAsTextFile(output)
