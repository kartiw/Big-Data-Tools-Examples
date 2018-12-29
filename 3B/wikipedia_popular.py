import sys
import os.path
from pyspark.sql import SparkSession, types, functions

spark = SparkSession.builder.appName('wikipedia_popular').getOrCreate()
sc = spark.sparkContext
assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
assert spark.version >= '2.2'  # make sure we have Spark 2.2+

inputs = sys.argv[1]
output = sys.argv[2]
#Function to extract filename
def filename(filename):
    filename = os.path.basename(filename)
    Req_Format = os.path.splitext(filename)[0].replace('pagecounts-','').replace('0000', '')
    return Req_Format

udf_filename = functions.udf(filename, returnType= types.StringType())
#Specifying Schema
schema = types.StructType([types.StructField('Language', types.StringType(), False),
                           types.StructField('PageName', types.StringType(), False),
                           types.StructField('View_Counts', types.IntegerType(), False),
                           types.StructField('Bytes', types.IntegerType(), False)])

Data = spark.read.csv(inputs, sep = ' ' ,schema= schema).withColumn('filename', udf_filename(functions.input_file_name()))
Data = Data.filter(Data['language'] == 'en')
Data = Data.filter(~Data['PageName'].startswith('Special:') & ~Data['PageName'].startswith('Main'))
CountDF= Data.groupby('filename').max('View_Counts').withColumnRenamed("max(View_Counts)","Max_ViewCount")
Joined_Data = Data.join(CountDF, 'filename')
Joined_Data = Joined_Data.select(CountDF['filename'],Data['PageName'],CountDF['Max_ViewCount']).where(CountDF["Max_ViewCount"] == Data["View_Counts"])
Final_Data = Joined_Data.orderBy("filename","pagename")
Final_Data.write.csv(output)
