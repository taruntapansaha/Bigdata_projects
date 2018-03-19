from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import numpy as np

if __name__ == "__main__":
    SparkContext.setSystemProperty('spark.executor.memory', '4g')
    sc = SparkContext(appName="Percentage_Delay_SequenceFile")
    sqlContext = SQLContext(sc)

    def airlineTuple(line):
      values = line.split(",")
      return (values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9],
              values[10], values[11], values[12], values[13], values[14], values[15], values[16], values[17], values[18], values[19],
              values[20], values[21], values[22], values[23], values[24], values[25], values[26], values[27], values[28])


    lines = sc.sequenceFile(sys.argv[1]).mapValues(airlineTuple)

    dataFrame = sqlContext.createDataFrame(lines.values(), ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime', 'ArrTime',
                                            'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'TailNum', 'ActualElapsedTime',
                                            'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Origin', 'Dest',
                                            'Distance', 'TaxiIn', 'TaxiOut', 'Cancelled', 'CancellationCode', 'Diverted',
                                            'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay',
                                            'LateAircraftDelay'])

    filteredDataFrame=dataFrame.fillna(np.nan)

    delayedOrigin=filteredDataFrame.filter(filteredDataFrame.DepDelay>0).select("Origin","DepDelay").groupBy(filteredDataFrame.Origin).count().orderBy(filteredDataFrame.Origin)
    totalCount=filteredDataFrame.groupBy(filteredDataFrame.Origin).count().orderBy(filteredDataFrame.Origin).selectExpr("Origin as totalOrigin","count as countTotal")
    delay_Count_Join=delayedOrigin.join(totalCount,totalCount.totalOrigin==delayedOrigin.Origin).orderBy(delayedOrigin.Origin)
    percentage=delay_Count_Join.selectExpr("totalOrigin","countTotal","count","(count/countTotal)*100 as Percentage_Delay")

    percentage.coalesce(1).rdd.saveAsTextFile(sys.argv[2])
