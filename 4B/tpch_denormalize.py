from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,functions
import pyspark_cassandra
from cassandra.cluster import Cluster
import sys

cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('example code').set('spark.cassandra.connection.host', ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.appName('temp ranges').getOrCreate()

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

tpch_keyspace = sys.argv[1]
my_keyspace = sys.argv[2]

cluster = Cluster(cluster_seeds)
session = cluster.connect(my_keyspace)
session.execute("""create table if not exists orders_parts 
                (orderkey int, 
                custkey int, 
                orderstatus text,
                totalprice decimal, 
                orderdate text, 
                order_priority text,
                clerk text, 
                ship_priority int,
                comment text, 
                part_names set<text>, 
                primary key(orderkey))""")

def df_for(keyspace, table, split_size=None):
    df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    df.createOrReplaceTempView(table)
    return df

orders = df_for(tpch_keyspace, 'orders')
orders.cache()

lineitem = df_for(tpch_keyspace, 'lineitem')
lineitem.cache()

part = df_for(tpch_keyspace, 'part')
part.cache()

merged_df = spark.sql("""SELECT o.*,p.name 
                      FROM orders o JOIN lineitem l 
                      ON (o.orderkey = l.orderkey) JOIN part p 
                      ON (l.partkey = p.partkey)""")

aggregate = merged_df.groupby('orderkey','totalprice').agg(functions.collect_list('name').alias('part_names'))
aggregate.createOrReplaceTempView('aggregate')

output_df = spark.sql("""SELECT o.*, a.part_names 
                      FROM orders o JOIN aggregate a 
                      ON (o.orderkey = a.orderkey)""")

output_df.rdd.map(lambda r: r.asDict()).saveToCassandra(my_keyspace, 'orders_parts')
