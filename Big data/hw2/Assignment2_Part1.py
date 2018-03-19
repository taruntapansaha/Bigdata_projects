
from pyspark import SparkConf, SparkContext
from operator import add
import sys
import re

def fileProcessor(x):
    fileName = x[0]
    fileName = re.sub('[\+]+', '', fileName).split("_")[-1]
    words = re.sub('[^A-Za-z]+', ' ', x[1].lower()).split()
    newWordList = [x + ' ' + fileName for x in words]
    return newWordList 
    

if __name__ == "__main__":
 
    sc = SparkContext(appName="WordCount")    
    
    counts = sc.wholeTextFiles(sys.argv[1]) \
                  .flatMap(fileProcessor) \
		  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    
    counts.saveAsTextFile("/bigd27/output")
    sc.stop()
