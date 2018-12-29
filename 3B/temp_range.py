#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 15 19:05:12 2017

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


myschema = types.StructType([
    types.StructField('WeatherStation', types.StringType(), False),
    types.StructField('date', types.IntegerType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False)
])

weather=spark.read.csv(inputs, myschema)
weather.createOrReplaceTempView('weather')

weather=weather.filter('qflag is NULL')
weather=weather.drop('mflag')
weather=weather.drop('qflag')

filter_for_Tmax=weather.filter("observation = 'TMAX'")
filter_for_Tmin=weather.filter("observation = 'TMIN'").withColumnRenamed("value","minval")

get_diff=filter_for_Tmax.join(filter_for_Tmin,on=['date','WeatherStation'], how='left')
get_diff=get_diff.withColumn("Range",get_diff.value - get_diff.minval)

get_max=get_diff.groupby("date").max('Range').withColumnRenamed("max(Range)","Range")
largest_temp_diff= get_max.join(get_diff, on=['date','Range'], how='left').orderBy('date').select("date",'WeatherStation','Range')

largest_temp_diff.write.csv(output,sep=" ")