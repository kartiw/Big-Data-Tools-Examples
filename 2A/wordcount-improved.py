# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import unicodedata

inputs = sys.argv[1]
output = sys.argv[2]
 
conf = SparkConf().setAppName('word count')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
#assert sys.version_info >= (3, 4)  # make sure we have Python 3.5+
#assert sc.version >= '2.2'  # make sure we have Spark 2.2+
 
def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line):
        if(w != ""):
            yield (unicodedata.normalize("NFD", w.lower()), 1)
 
def get_key(kv):
    return kv[0]

def freq(kv):
    return kv[1]
 
def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)
 
text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(operator.add)
#outdata = wordcount
wordcount = wordcount.cache()
outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output+"/by-word")

#statements to sort by frequency, will create a second output file
outdata2 = wordcount.sortBy(get_key).sortBy(freq,False).map(output_format)
outdata2.saveAsTextFile(output+"/by-freq")

sc.stop()