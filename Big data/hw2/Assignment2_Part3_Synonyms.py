from __future__ import print_function
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.feature import Word2Vec
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
import operator
from operator import add
import sys
import re

def fileParser(x):
    if x not in stopwords:
        x=re.sub('[^A-Za-z]+', '', x.lower())
        return x
if __name__ == "__main__":

    sc = SparkContext(appName="Word2VectorPart1")

    stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an",
                "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before",
                "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
                "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down",
                "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
                "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
                "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
                "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me",
                "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
                "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same",
                "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
                "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
                "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those",
                "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
                "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", 
                "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
                "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", 
                "you've", "your", "yours", "yourself", "yourselves"]

    initialRDD = sc.textFile(sys.argv[1]).map(lambda row: row.split(" "))

    topTenElements = sc.textFile(sys.argv[1], use_unicode=False)\
                        .flatMap(lambda line: line.split())\
                        .filter(lambda x: fileParser(x))\
                        .map(lambda w: (w, 1))\
                        .reduceByKey(add)\
                        .map(lambda (x, y): (y, x))\
                        .sortByKey(False)\
                        .take(10)

    word2vec = Word2Vec()
    model = word2vec.fit(initialRDD)

    finalList = map(operator.itemgetter(1), topTenElements)

    FinalListOfSynonyms=''

    for word in finalList:
        FinalListOfSynonyms += 'Word: ' + "\n" + str(word).upper() + "\n" + 'Synonyms are' + "\n"
        synonyms = model.findSynonyms(word, 5)

        for synonym, cosine_distance in synonyms:
            FinalListOfSynonyms += str(("{}: {}".format(synonym, cosine_distance)) + "\n")

    sc.parallelize([FinalListOfSynonyms]).saveAsTextFile("/bigd27/output")
    sc.stop()
