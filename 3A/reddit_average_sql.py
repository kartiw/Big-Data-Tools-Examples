#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 10:23:00 2017

@author: kartiw
"""

import sys
from pyspark.sql import SparkSession, functions, types, SQLContext
  
spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.2' # make sure we have Spark 2.2+
sqlContext = SQLContext(sc)

def main():
    inputs = sys.argv[1]
    output = sys.argv[2]
    
    schema = types.StructType([
    types.StructField('subreddit', types.StringType(), False),
    types.StructField('score', types.FloatType(), False)
])
    comments = sqlContext.read.json(inputs , schema)
    #comments = spark.read.json(inputs)
    comments.createOrReplaceTempView('comments')
    averages = spark.sql("""
    SELECT subreddit, AVG(score)
    FROM comments
    GROUP BY subreddit
    """)
    averages.show() #showing output since many op files are created
    averages.write.save(output, format='json', mode='overwrite')
    
if __name__ == "__main__":
    main()