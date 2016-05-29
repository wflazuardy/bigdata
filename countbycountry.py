from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Killer")
sc = SparkContext(conf = conf)

maxValue = 0
maxIndex = "nothing"

def parseLine(line):
    fields = line.split(',')
    country = fields[6]
    return (country)

lines = sc.textFile("execution_database.csv")
rdd = lines.map(parseLine)
#count = lines.map(lambda x: x.split()[8])
result = rdd.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i" % (key, value)
    if maxValue < value:
        maxValue = value
        maxIndex = key

print ("\nCountry dengan jumlah kasus kekerasan terbanyak adalah:")
print "%s " % (maxIndex) + "dengan jumlah kasus sebanyak " + "%i "%(maxValue) + "kali"
#stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

    
#print "\nCountry yang paling sering terjadi kasus pembunuhan adalah "+ maxVal