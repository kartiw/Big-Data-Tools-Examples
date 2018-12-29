from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,functions
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
order_keys = sys.argv[3:]

def df_for(keyspace, table, split_size=None):
    df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    df.createOrReplaceTempView(table)
    return df

def output_format(out):
    return "Order #"+str(out[0])+" $"+str(float("{0:.2f}".format(round(out[1],2))))+":"+str(out[2])

orders = df_for(keyspace, 'orders')
lineitem = df_for(keyspace, 'lineitem')
part = df_for(keyspace, 'part')

orders.cache()
lineitem.cache()
part.cache()

merged_df = spark.sql("""SELECT o.orderkey, o.totalprice,p.name 
                      FROM orders o JOIN lineitem l 
                      ON (o.orderkey = l.orderkey) JOIN part p 
                      ON (l.partkey = p.partkey) 
                      WHERE o.orderkey IN (""" + ','.join(order_keys) + ")")

output_df = merged_df.groupby('orderkey','totalprice').agg(functions.concat_ws(',' , functions.collect_list('name')).alias('part_names'))
out_rdd = output_df.rdd.map(tuple).map(output_format)
#print(out_rdd.take(5))
out_rdd.saveAsTextFile(output)