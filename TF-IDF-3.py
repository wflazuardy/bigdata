import re
import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF


#print (raw_input('Enter the keyword : '))
#x = raw_input('Enter the keyword : ')
# Function for printing each element in RDD
def println(x):
    print x

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

#print word

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents (one per line).
rawData = sc.textFile("Berita.tsv")
#print rawData
#words = rawData.map(normalizeWords)
fields = rawData.map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[2].lower().split(" "))
#words = str(documents)
#print words

#print documents
#words = documents.flatmap(normalizeWords)
documentID = fields.map(lambda x: x[0])

#x =re.findall('isis', documents, flags=re.IGNORECASE)
#print x

hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(documents)


tf.cache()
idf = IDF(minDocFreq=1).fit(tf)
tfidf = idf.transform(tf)


word = sys.argv[1]
wordx = normalizeWords(word)

keywordTF = hashingTF.transform(wordx)
keywordHashValue = int(keywordTF.indices[0])

keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

zippedResults = keywordRelevance.zip(documentID)

print "Best document for keywords is:"
print zippedResults.max()