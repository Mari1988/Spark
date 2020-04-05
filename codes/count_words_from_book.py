import os
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.6"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.6"

from pyspark import SparkConf, SparkContext

# create spark-context
conf = SparkConf().setMaster('local').setAppName('countWords')
sc = SparkContext(conf=conf)

# load the weather file into a RDD
book = sc.textFile("file:///home/mariappan/Mari/git-repos/Spark/datasets/frank_book.txt")

# flat-map splits each line from the book into words and make each word a line
words_rdd = book.flatMap(lambda x: x.split())

# create key-value RDD where words are the keys
kv_rdd = words_rdd.map(lambda x: (x, 1))

# sum up the values
counts = kv_rdd.reduceByKey(lambda x, y: x+y)

results = counts.collect()

# simpler way
results_2 = kv_rdd.countByValue()

sc.stop()

# http://192.168.1.4:4040/executors/
