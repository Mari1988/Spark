from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# os.chdir('/home/mariappan/Mari/git-repos/Spark/')
lines = sc.textFile("file:///home/mariappan/Mari/git-repos/Spark/datasets/ml-100k/u.data")  # RDD creation
ratings = lines.map(lambda x: x.split()[2])  # RDD transformation
result = ratings.countByValue()  # RDD action

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
