from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import sys

sc = SparkContext(appName="File_Converter")
sqlContext = SQLContext(sc)


# converts a line into tuple
def airlineTuple(line):
    values = line.split(",")

    return (
        values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9],
        values[10], values[11], values[12], values[13], values[14], values[15], values[16], values[17], values[18],
        values[19], values[20], values[21], values[22], values[23], values[24], values[25], values[26], values[27], values[28])


# load the airline data and covert into an RDD of tuples
input = sc.textFile(sys.argv[1]).map(airlineTuple)

# convert the rdd into a dataframe
dataFrame = sqlContext.createDataFrame(input, ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime', 'ArrTime',
                                        'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'TailNum', 'ActualElapsedTime',
                                        'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Origin', 'Dest',
                                        'Distance', 'TaxiIn', 'TaxiOut', 'Cancelled', 'CancellationCode', 'Diverted',
                                        'CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay',
                                        'LateAircraftDelay'])

# save the dataframe as a parquet file in HDFS
dataFrame.write.parquet(sys.argv[2] + "/ParquetFile");

# save the dataframe as a json file in HDFS
dataFrame.write.json(sys.argv[2] + "/JsonFile");

# save the dataframe as a sequence file in HDFS
sequenc_Input = sc.textFile(sys.argv[1]).map(lambda x: (None, x))
sequenc_Input.saveAsSequenceFile(sys.argv[2] + "/SequenceFile");
