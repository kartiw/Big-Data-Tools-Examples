
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

import sys, os,gzip
import datetime
import re
import uuid
from datetime import datetime

inputs = sys.argv[1]
keyspace = sys.argv[2]
tablename = sys.argv[3]

cluster = Cluster(['199.60.17.171', '199.60.17.188'])
#cluster = Cluster()
session = cluster.connect(keyspace)

#check if the table exists or not and creates the table
try:
    session.execute("DROP TABLE {0}".format(tablename))
    session.execute("CREATE TABLE {0} (host TEXT, identifier UUID, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (identifier, host))".format(tablename))
except:
    session.execute("CREATE TABLE {0} (host TEXT, identifier UUID, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (identifier, host))".format(tablename))


def words_once(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    splitted_line=line_re.split(line)
    #yield(len(splitted_line))
    if len(splitted_line) > 5:
        date = datetime.strptime(splitted_line[2], '%d/%b/%Y:%H:%M:%S')
        return(splitted_line[1], date, splitted_line[3], float(splitted_line[4]))
        
Insert = session.prepare("INSERT INTO nasalogs (host,identifier,datetime,path,bytes) VALUES (?,?,?,?,?)")
counter = 0
batchSize =  200


for f in os.listdir(inputs):
    filename = os.path.join(inputs, f)
    filepath, file_ext = os.path.splitext(filename)
    if file_ext ==".gz":
        file = gzip.open(filename, 'rt', encoding='utf-8', errors='ignore')
    else:
        file = open(filename)
        
    with file as logfile:
        batch_obj = BatchStatement()
        
        for line in logfile:
            logs = words_once(line)
            if logs is not None:
                batch_obj.add(SimpleStatement("INSERT INTO {0} (host,datetime,path,bytes,identifier) VALUES (%s,%s,%s,%s,%s)".format(tablename)),(logs[0],logs[1], logs[2], int(logs[3]), uuid.uuid1()))
                counter += 1
            if counter == batchSize:
                session.execute(batch_obj)
                counter = 0
                batch_obj = BatchStatement()
        session.execute(batch_obj)