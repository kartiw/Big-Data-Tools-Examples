#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 16:14:54 2017

@author: kartiw
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, SQLContext, Row
from pyspark.sql.functions import *

import sys

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('ingest logs')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.appName('example application').getOrCreate()
sqlContext = SQLContext(sc)

def get_hour(df):
    hour=[]
    filenames=df.select('filename').collect()
    for i in range(len(filenames)):
        hour.append(filenames[i][0].split("/")[-1][11:22])
    return hour

myschema = types.StructType([
    types.StructField('lang', types.StringType(), False),
    types.StructField('page_name', types.StringType(), False),
    types.StructField('times_requested', types.IntegerType(), False),
    types.StructField('bytes', types.IntegerType(), False)
])

wiki=spark.read.csv(inputs, myschema, sep=" ").withColumn('filename', functions.input_file_name())

filtered_wiki=wiki.where((wiki.lang == "en") & (wiki.page_name != 'Main_Page') & ~(wiki.page_name.like("%Special:%"))) 
path_to_hour = functions.udf(get_hour(filtered_wiki), returnType=types.StringType())