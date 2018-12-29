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

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('ingest logs')
#sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.appName('example application').getOrCreate()
sqlContext = SQLContext(sc)


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
weather=sqlContext.sql("""SELECT WeatherStation, date, observation, value FROM weather WHERE qflag IS NULL""")
weather.createOrReplaceTempView('weather')

filter_for_Tmax = sqlContext.sql("""SELECT * FROM weather where observation='TMAX'""")
filter_for_Tmax.createOrReplaceTempView('filter_for_Tmax')

filter_for_Tmin = sqlContext.sql("""SELECT * FROM weather where observation='TMIN'""")
filter_for_Tmin.createOrReplaceTempView('filter_for_Tmin')

get_diff= sqlContext.sql("""SELECT tmax.date, tmax.WeatherStation, (tmax.value - tmin.value) AS range 
                    FROM filter_for_Tmax AS tmax 
                    LEFT JOIN filter_for_Tmin AS tmin
                    ON tmax.date = tmin.date AND tmax.WeatherStation = tmin.WeatherStation""")
get_diff.createOrReplaceTempView('get_diff')

get_max=sqlContext.sql("""SELECT date, MAX(range) as range FROM get_diff GROUP BY date""")
get_max.createOrReplaceTempView('get_max')

largest_temp_diff=sqlContext.sql("""SELECT gm.date, gd.WeatherStation, gm.range
                            FROM get_max as gm
                            LEFT JOIN get_diff as gd
                            ON gm.date = gd.date AND gm.range = gd.range
                            ORDER BY date""")

#largest_temp_diff.write.csv(output,sep=" ")