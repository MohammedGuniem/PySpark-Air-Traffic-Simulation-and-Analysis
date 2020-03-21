from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws, mean as _mean, max as _max, desc

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

TARGET = {
 'year': '2019',
 'month': '03',
 'day': '08'
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

print("Writing csv file on local machine ...")
routes_df.repartition(1).write.csv("debug_1", header = 'true')
