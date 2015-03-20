#import sys
#from operator import add
#from pyspark import SparkContext
#sc = SparkContext(appName="wordCount")
#lines = sc.textFile(sys.argv[1], 1)
# --------------------------------------------------------------------
# --------------------------------------------------------------------

# Summary: Working of a Spark Program

# 1. ETL -> Create RDD from external file
lines = sc.textFile("readme.txt",1)

# --------------------------------------------------
# 2. Create RDD via Transformation of previous RDD
# filter() transformation
## passing function to Transformation: lambda-function
musicLines = lines.filter(lambda line: "music" in line)	

## passing function to Transformation: local function 
def containsVoice(s):
	return "voice" in s
	
voiceLines = lines.filter(containsVoice)

# union() transformation
goodLines = musicLines.union(voiceLines)

# --------------------------------------------------
# 3. store in memory, to enable multiple Actions!
musicLines.persist()		

# 4. Call an ACTION! Evaluate above Transformations!
count = musicLines.count()

print "Input had" + count + "lines with word 'Music'"
print "Here are 10 examples"
for line in musicLines.take(10):
	print line

# flatMap() transformation: Produce Multiple Output elements for each input Element.
# output: RDD consisting of the elements of elements from all the iterators.
words = musicLines.flatMap(lambda line: line.split(''))
words.first()

musicCounts = musicLines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
	
sc.stop()
# --------------------------------------------------------------------
# END
# --------------------------------------------------------------------

#output = counts.collect()
#for (word, count) in output:
#	print "%s: %i" % (word, count)


	
