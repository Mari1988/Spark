import os
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3.6"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3.6"

from pyspark import SparkConf, SparkContext

# create spark-context
conf = SparkConf().setMaster('local').setAppName('MinTempByStation')
sc = SparkContext(conf=conf)

# load the weather file into a RDD
weather = sc.textFile("file:///home/mariappan/Mari/git-repos/Spark/datasets/1800.csv")

# get only the relevant info
def parse_lines(lines):
    fields = lines.split(',')
    stationID = fields[0]
    eventType = fields[2]
    temp = float(fields[3]) * .1 * (9.0/5.0) + 32
    return (stationID, eventType, temp)

rdd = weather.map(parse_lines)

# filter out the records which doesn't include TMIN
tmin_rdd = rdd.filter(lambda x: 'TMIN' in x[1])

# create key-value RDD by stripping out eventType
kv_rdd = tmin_rdd.map(lambda x: (x[0], x[2]))

# reduceBy (aggregate and find the minimum temperature for each station)
min_temp_by_station = kv_rdd.reduceByKey(lambda x, y: min(x, y))

# get the result (RDD action)
out = min_temp_by_station.collect()

for element in out:
    print("Station ID {} and the minimum temperature is {:.2f}F".format(element[0], element[1]))

# http://192.168.1.4:4040/executors/
sc.stop()