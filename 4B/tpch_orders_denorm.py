from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,functions,Row
import pyspark_cassandra
import sys

cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('example code').set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.appName('temp ranges').getOrCreate()

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

keyspace = sys.argv[1]
output = sys.argv[2]
orderkeys = sys.argv[3:]

def rdd_for(keyspace, table, split_size=None):
    rdd = sc.cassandraTable(keyspace, table, split_size=split_size,
        row_format=pyspark_cassandra.RowFormat.TUPLE).select('orderkey', 'totalprice', 'part_names').where("orderkey in ?",orderkeys).setName(table)
    return rdd

def output_format(out):
    return "Order #"+str(out[0])+" $"+str(float("{0:.2f}".format(round(out[1],2))))+":"+str(out[2])

rdd = rdd_for(keyspace, 'orders_parts').map(lambda x:(x[0],x[1], ','.join(map(str,list(x[2]))))).map(output_format)
rdd.saveAsTextFile(output)