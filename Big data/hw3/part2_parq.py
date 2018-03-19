from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import numpy as np

if __name__ == "__main__":
    SparkContext.setSystemProperty('spark.executor.memory', '4g')
    sc = SparkContext(appName="Percentage_Delay_ParquetFile")
    sqlContext = SQLContext(sc)

    dataFrame=sqlContext.read.parquet(sys.argv[1])

    filteredDataFrame=dataFrame.fillna(np.nan)

    delayedOrigin=filteredDataFrame.filter(filteredDataFrame.DepDelay>0).select("Origin","DepDelay").groupBy(filteredDataFrame.Origin).count().orderBy(filteredDataFrame.Origin)
    totalCount=filteredDataFrame.groupBy(filteredDataFrame.Origin).count().orderBy(filteredDataFrame.Origin).selectExpr("Origin as totalOrigin","count as countTotal")
    
    delay_Count_Join=delayedOrigin.join(totalCount,totalCount.totalOrigin==delayedOrigin.Origin).orderBy(delayedOrigin.Origin)
    percentage=delay_Count_Join.selectExpr("totalOrigin","countTotal","count","(count/countTotal)*100 as Percentage_Delay")

    percentage.coalesce(1).rdd.saveAsTextFile(sys.argv[2])
