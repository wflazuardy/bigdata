from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    method = fields[8]
    return (method)

lines = sc.textFile("file:///SparkCourse/execution_database.csv")
rdd = lines.map(parseLine)
count = lines.map(lambda x: x.split()[8])
result = rdd.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i" % (key, value)
