#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  3 15:29:34 2017

@author: kartiw
"""

from pyspark import SparkConf, SparkContext
import sys
import operator
import re

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('correlate logs')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)

def words_once(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    splitted_line=line_re.split(line)
    #yield(len(splitted_line))
    if len(splitted_line) > 5:
        yield(splitted_line[1], (float(1), float(splitted_line[4])))
            
def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def add_pairs(kv0,kv1):
    tbytes=count=0
    tbytes=kv0[1]+kv1[1]
    count=kv0[0]+kv1[0]
    return count,tbytes

            
text = sc.textFile(inputs)
words = text.flatMap(words_once)

reducedkeyrdd = words.reduceByKey(add_pairs)
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
#newrdd.saveAsTextFile(output)



