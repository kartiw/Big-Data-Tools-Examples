import sys
from pyspark.sql import SparkSession, types, functions

spark = SparkSession.builder.appName('shortest_path').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+


inputs = sys.argv[1]
output = sys.argv[2]
source = sys.argv[3]
dest = sys.argv[4]

schema = types.StructType([types.StructField('Node', types.IntegerType(), False),
                           types.StructField('Destination',types.StringType(), False)])

node_schema = types.StructType([types.StructField('Node', types.IntegerType(), False),
                           types.StructField('Source',types.IntegerType(), False),
                            types.StructField('Distance', types.IntegerType(), False)])

Data = spark.read.csv(inputs, schema= schema, sep = ':')
node_split_list = Data.withColumn("Edge" , functions.explode(functions.split(Data['Destination'],(" "))))
non_empty_node_list = node_split_list.filter(~(node_split_list['Edge'] == "")).select(node_split_list['Node'], node_split_list['Edge'])

node_list = spark.createDataFrame([(int(source),-1,0)],schema = node_schema)

for i in range(6):
    new_df =  node_list.join(non_empty_node_list, node_list['Node'] == non_empty_node_list['Node'])
    new_df = new_df.select(non_empty_node_list['Edge'].alias('Node'),non_empty_node_list['node'].alias('Source'),((node_list['Distance'])+1).alias("Distance"))
    node_list =  node_list.unionAll(new_df).orderBy('Distance')
    Path = node_list.dropDuplicates(['node'])
    dest_check = Path.select(Path['node']).where(Path['node'] == dest).count()
    if dest_check == 1:
        Path = Path.orderBy('Distance')
        break

Path.createOrReplaceTempView("Path")
output_MSG = ""
dest_source = spark.sql("""SELECT Source from Path where Node=""" + str(dest))
if (dest_source.count() != 0):
    dest_source = dest_source.rdd.flatMap(list).first()
    l = [dest]
    while (dest_source != -1):
        l += [dest_source]
        dest_source = spark.sql("""SELECT Source from Path where Node=""" + str(dest_source)).rdd.flatMap(list).first()
    l = list(reversed(l))
    for i in l:
        output_MSG += str(i) + '\n'
else:
    output_MSG = "Path not found."
sc.parallelize([output_MSG]).saveAsTextFile(output)



