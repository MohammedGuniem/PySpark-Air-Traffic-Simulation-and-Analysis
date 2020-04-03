from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws, mean as _mean, max as _max, desc, udf, col, lit
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, DoubleType
from datetime import datetime, timedelta
import time
from mpl_toolkits.basemap import Basemap

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

now = datetime.now()
print("Started at: ", now)

scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/*.csv")
print("Count of rows: ", scaling_df.count())

print("Removing flights that never been in the sky...")
scaling_df = scaling_df.na.drop(subset=["AIR_TIME"])
print("Count of rows after removing: ", scaling_df.count())

print("Removing flights that never took off...")
scaling_df = scaling_df.filter(scaling_df.WHEELS_OFF.between(0000, 2359))
print("Count of rows after removing: ", scaling_df.count())

print("Removing flights that never landed...")
scaling_df = scaling_df.filter(scaling_df.WHEELS_ON.between(0000, 2359))
print("Count of rows after removing: ", scaling_df.count())

print("Adding latitude, longitude and UTC time to the origin and destination of each route...")
airports_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/support_data/airports_data.csv")
airports_df = airports_df.withColumnRenamed('AIRPORT_SEQ_ID','ORIGIN_AIRPORT_SEQ_ID')
airports_df = airports_df.withColumn("DEST_AIRPORT_SEQ_ID", airports_df["ORIGIN_AIRPORT_SEQ_ID"])
print("Count of rows in support data: ", airports_df.count())

print("Joining the scaling dataframe with the routes information from the support data...")
scaling_df = scaling_df.join(airports_df.select(*["ORIGIN_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE","UTC_LOCAL_TIME_VARIATION"]), "ORIGIN_AIRPORT_SEQ_ID")
scaling_df = scaling_df.withColumnRenamed('LATITUDE','ORIGIN_LATITUDE')
scaling_df = scaling_df.withColumnRenamed('LONGITUDE','ORIGIN_LONGITUDE')
scaling_df = scaling_df.withColumnRenamed('UTC_LOCAL_TIME_VARIATION','ORIGIN_UTC_LOCAL_TIME_VARIATION')
print("Count of rows after adding support data: ", scaling_df.count())

scaling_df = scaling_df.join(airports_df.select(*["DEST_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE","UTC_LOCAL_TIME_VARIATION"]), "DEST_AIRPORT_SEQ_ID")
scaling_df = scaling_df.withColumnRenamed('LATITUDE','DEST_LATITUDE')
scaling_df = scaling_df.withColumnRenamed('LONGITUDE','DEST_LONGITUDE')
scaling_df = scaling_df.withColumnRenamed('UTC_LOCAL_TIME_VARIATION','DEST_UTC_LOCAL_TIME_VARIATION')
print("Count of rows after adding support data: ", scaling_df.count())

print("Converting wheels off and wheels on from local time to UTC time...")
def convert_local_to_UTC_time(date, time, utc_time_variation):
     digit_count = len(str(time))
     if digit_count == 1:
         time = "000"+str(time)
     elif digit_count == 2:
         time = "00"+str(time)
     elif digit_count == 3:
         time = "0"+str(time)
     else:
         time = str(time)
     pattern_string = '%Y-%m-%d %H%M'
     date = str(date).split(" ")[0]
     datetime_string = date + ' ' + str(time)
     date_and_time = datetime.strptime(datetime_string, pattern_string)
     time_in_utc = date_and_time - timedelta(hours=(int(utc_time_variation)/100))
     return time_in_utc

convert_local_to_UTC_time_udf = udf(convert_local_to_UTC_time, TimestampType())

scaling_df = scaling_df.withColumn("WHEELS_OFF_UTC_DATETIME", convert_local_to_UTC_time_udf(scaling_df["FL_DATE"].cast(StringType()), scaling_df["WHEELS_OFF"], scaling_df["ORIGIN_UTC_LOCAL_TIME_VARIATION"]))
print("Count of rows after converting wheels off to utc time: ", scaling_df.count())

scaling_df = scaling_df.withColumn("WHEELS_ON_UTC_DATETIME", convert_local_to_UTC_time_udf(scaling_df["FL_DATE"].cast(StringType()), scaling_df["WHEELS_ON"], scaling_df["DEST_UTC_LOCAL_TIME_VARIATION"]))
print("Count of rows after converting wheels on to utc time: ", scaling_df.count())

print("Writing prepared data to local filesystem")
scaling_df.repartition(1).write.csv("prepared_data", header = 'true')

print("Writing prepared data to HDFS filesystem")
scaling_df.repartition(1).write.csv("hdfs://master:9000/scaling_and_simulation/prepared_data", header = 'true')

now = datetime.now()
print("Finished at: ", now)
