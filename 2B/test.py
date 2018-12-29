#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 13:26:52 2017

@author: kartiw

"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('correlate logs')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)

def make_tuple(line):
    return (line['g'],(line['a'],line['b']))
    
    
test=[{'g':'k1','a':5,'b':10},{'g':'k1','a':1,'b':2},{'g':'k2','a':11,'b':12}]

for i in test:
    print(make_tuple(i))

#test =sc.parallelize(test)
#t = test.map(make_tuple)
#t.take(1)