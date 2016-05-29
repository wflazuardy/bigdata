from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Killer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    killer = fields[4]
    victim = fields[5]
    return (killer, victim)

lines = sc.textFile("execution_database.csv")
rdd = lines.map(parseLine)
#count = lines.map(lambda x: x.split()[8])
result = rdd.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i" % (key, value)