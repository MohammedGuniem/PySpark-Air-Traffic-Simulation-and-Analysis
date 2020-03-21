from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws, mean as _mean, max as _max, desc, udf, col, lit
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime, timedelta
import time


sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

TARGET = {
 'year': '2019',
 'month': '03',
 'day': '08',
 'from_utc_time': '15:30:00',
 'to_utc_time': '21:45:00',
 'output_filename': 'routes_on_2019_03_08_From_1530_To_2145'
}

print("Started scaling ...")
scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/"+TARGET['year']+"_"+TARGET['month']+".csv")
print("Count of rows: ", scaling_df.count())

print("Filter in respect to day of month -> keeping yesterday, today and tomorrow... ")
scaling_df = scaling_df.filter(scaling_df.DAY_OF_MONTH.between(int(TARGET['day'])-1, int(TARGET['day'])+1))
print("Count of rows: ", scaling_df.count())

print("Constructing the route paths in one column...")
scaling_df = scaling_df.withColumn("ROUTE_PATH", concat_ws("->", "ORIGIN_AIRPORT_SEQ_ID", "DEST_AIRPORT_SEQ_ID"))
print("Count of rows: ", scaling_df.count())

print("Calculating the mean of air time and distance for each route...")
routes_df = scaling_df.groupBy('ROUTE_PATH')
routes_df = routes_df.agg(_max('ORIGIN_AIRPORT_SEQ_ID').alias('ORIGIN_AIRPORT_SEQ_ID'), 
                          _max('DEST_AIRPORT_SEQ_ID').alias('DEST_AIRPORT_SEQ_ID'), 
                          _mean('AIR_TIME').alias('AIR_TIME_AVG'), 
                          _mean('DISTANCE').alias('DISTANCE')).sort(desc("ROUTE_PATH"))
print("Count of rows: ", routes_df.count())

print("Adding latitude, longitude and UTC time to the origin and destination of each route...")
airports_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/support_data/airports_data.csv")
airports_df = airports_df.withColumnRenamed('AIRPORT_SEQ_ID','ORIGIN_AIRPORT_SEQ_ID')
airports_df = airports_df.withColumn("DEST_AIRPORT_SEQ_ID", airports_df["ORIGIN_AIRPORT_SEQ_ID"])

routes_df = routes_df.join(airports_df.select(*["ORIGIN_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE","UTC_LOCAL_TIME_VARIATION"]), "ORIGIN_AIRPORT_SEQ_ID")
routes_df = routes_df.withColumnRenamed('LATITUDE','ORIGIN_LATITUDE')
routes_df = routes_df.withColumnRenamed('LONGITUDE','ORIGIN_LONGITUDE')
routes_df = routes_df.withColumnRenamed('UTC_LOCAL_TIME_VARIATION','ORIGIN_UTC_LOCAL_TIME_VARIATION')

routes_df = routes_df.join(airports_df.select(*["DEST_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE","UTC_LOCAL_TIME_VARIATION"]), "DEST_AIRPORT_SEQ_ID")
routes_df = routes_df.withColumnRenamed('LATITUDE','DEST_LATITUDE')
routes_df = routes_df.withColumnRenamed('LONGITUDE','DEST_LONGITUDE')
routes_df = routes_df.withColumnRenamed('UTC_LOCAL_TIME_VARIATION','DEST_UTC_LOCAL_TIME_VARIATION')
print("Count of rows: ", routes_df.count())

print("Joining the scaling dataframe with the routes information dataframe...")
scaling_df = scaling_df.join(routes_df.select(*["ROUTE_PATH",'ORIGIN_LATITUDE','ORIGIN_LONGITUDE','ORIGIN_UTC_LOCAL_TIME_VARIATION','DEST_LATITUDE','DEST_LONGITUDE','DEST_UTC_LOCAL_TIME_VARIATION']), "ROUTE_PATH")
print("Count of rows: ", scaling_df.count())

print("Removing flights that never took off or landed...")
scaling_df = scaling_df.filter(scaling_df.WHEELS_OFF.between(0000, 2359))
print("Count of rows: ", scaling_df.count())
scaling_df = scaling_df.filter(scaling_df.WHEELS_ON.between(0000, 2359))
print("Count of rows: ", scaling_df.count())

print("Converting wheels off and wheels on local time to UTC time...")
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
print("Count of rows: ", scaling_df.count())
scaling_df = scaling_df.withColumn("WHEELS_ON_UTC_DATETIME", convert_local_to_UTC_time_udf(scaling_df["FL_DATE"].cast(StringType()), scaling_df["WHEELS_ON"], scaling_df["DEST_UTC_LOCAL_TIME_VARIATION"]))
print("Count of rows: ", scaling_df.count())

print("Filter in respect to time... ")
utc_from_datetime = TARGET['year'] + "-" + TARGET['month'] + "-" + TARGET['day'] + " " + TARGET['from_utc_time']
utc_to_datetime = TARGET['year'] + "-" + TARGET['month'] + "-" + TARGET['day'] + " " + TARGET['to_utc_time']
scaling_df = scaling_df.where(col('WHEELS_OFF_UTC_DATETIME').between(*(utc_from_datetime.replace(TARGET['year'],str(int(TARGET['year'])-1)), utc_to_datetime)))
print("Count of rows: ", scaling_df.count())
scaling_df = scaling_df.where(col('WHEELS_ON_UTC_DATETIME').between(*(utc_from_datetime, utc_to_datetime.replace(TARGET['year'],str(int(TARGET['year'])+1)))))
print("Count of rows: ", scaling_df.count())

print("Writing csv file on local machine ...")
scaling_df.repartition(1).write.csv(TARGET['output_filename'], header = 'true')
