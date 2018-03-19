from __future__ import print_function
import sys
from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import Tokenizer, StopWordsRemover, Word2Vec
from pyspark.sql import SQLContext, Row

if __name__ == "__main__":

    sc = SparkContext(appName="Word2VecPart1")
    sqlContext = SQLContext(sc)

    inputRDD = sc.wholeTextFiles(sys.argv[1])
    wordDataFrame = sqlContext.createDataFrame(inputRDD, ["fileName", "content"])

    tokenizer = Tokenizer(inputCol="content", outputCol="word")
    wordsDF = tokenizer.transform(wordDataFrame)

    stopWordRemover = StopWordsRemover(inputCol="word", outputCol="filtered")
    filteredDataFrame = stopWordRemover.transform(wordsDF)

    word2Vec = Word2Vec(vectorSize=300, minCount=2, inputCol="filtered", outputCol="word2Vec")
    model = word2Vec.fit(filteredDataFrame)
    
    output = model.transform(filteredDataFrame)
    output.select("filtered", "word2Vec").rdd.saveAsTextFile("/bigd27/output")
    sc.stop()
