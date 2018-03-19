
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, NGram, StopWordsRemover
import sys

if __name__ == "__main__":
    SparkContext.setSystemProperty('spark.executor.memory', '2g')
    sc = SparkContext(appName="TF-IDF_Bigram")
    sqlContext = SQLContext(sc)

    fileRDD = sc.wholeTextFiles(sys.argv[1])
    dataFrame = sqlContext.createDataFrame(fileRDD, ["fileName", "content"])

    tokenizer = Tokenizer(inputCol="content", outputCol="words")
    wordDataFrame = tokenizer.transform(dataFrame)
    
    stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="filteredData")
    filteredDataFrame = stopWordsRemover.transform(wordDataFrame)
    
    ngram = NGram(n=2,inputCol="words", outputCol="bigrams")
    bigramDataFrame = ngram.transform(filteredDataFrame)

    hashingTF = HashingTF(inputCol="bigrams", outputCol="rawFeatures", numFeatures=100000)
    hasingData = hashingTF.transform(bigramDataFrame)

    idf = IDF(inputCol="rawFeatures", outputCol="TF-IDF_Bigram")
    idf_fit = idf.fit(hasingData)
    
    finalData = idf_fit.transform(hasingData)
    finalData.select("fileName", "bigrams", "TF-IDF_Bigram").rdd.saveAsTextFile("/bigd27/output")
