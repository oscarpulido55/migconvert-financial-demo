import pyspark
from pyspark.context import SparkContext
spc = SparkContext("spark://mycluster:7077")

hdfs_host = "myhost.me.com"
hdfs_port = 8020
hdfs_path = "/user/me/input"

import os
text_file = spc.textFile("hdfs://%s:%d%s" % (hdfs_host, hdfs_port, os.path.join("/", hdfs_path)))
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
values = counts.collect()
if len(values) > 20:
    values = values[:20]
print("\n".join([x[0] + " = " + str(x[1]) for x in values]))