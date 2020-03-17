from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SQLContext
from mpl_toolkits.basemap import Basemap

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

print("Started scaling ...")

scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/2019_03.csv")

airports_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/support_data/airports_data.csv")

airports_df = airports_df.withColumnRenamed('AIRPORT_SEQ_ID','ORIGIN_AIRPORT_SEQ_ID')
airports_df = airports_df.withColumn("DEST_AIRPORT_SEQ_ID", airports_df["ORIGIN_AIRPORT_SEQ_ID"])

print("total number of data row before joining with coordinates: ", scaling_df.count()) 

scaling_df = scaling_df.join(airports_df.select(*["ORIGIN_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE"]), "ORIGIN_AIRPORT_SEQ_ID")
scaling_df = scaling_df.withColumnRenamed('LATITUDE','ORIGIN_LATITUDE')
scaling_df = scaling_df.withColumnRenamed('LONGITUDE','ORIGIN_LONGITUDE')

scaling_df = scaling_df.join(airports_df.select(*["DEST_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE"]), "DEST_AIRPORT_SEQ_ID")
scaling_df = scaling_df.withColumnRenamed('LATITUDE','DEST_LATITUDE')
scaling_df = scaling_df.withColumnRenamed('LONGITUDE','DEST_LONGITUDE')

print("total number of data row after joining with coordinates: ", scaling_df.count())

showRowCount = 10 

print("Initial number of rows in dataframe: ", scaling_df.count())

scaling_df = scaling_df.filter(scaling_df.FL_DATE == "2019-03-08")

print("count after filtering in respect to date: ", scaling_df.count()) 

scaling_df = scaling_df.filter(scaling_df.WHEELS_OFF.between(1720, 1840))

scaling_df = scaling_df.filter(scaling_df.WHEELS_ON.between(1720,1840))

print("count after filtering between departure time and arrival time: ", scaling_df.count())

def find_route_path(origin_lat, origin_lon, destination_lat, destination_lon, distance_in_miles):    
    area_map = Basemap(llcrnrlon=-109, llcrnrlat=37, urcrnrlon=-102, urcrnrlat=41, lat_ts=0, resolution='l')    
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat, (distance_in_miles/40)+1)
    for lon in Longs:
        if float(lon) < -102 and float(lon) > -109 and float(Lats[Longs.index(lon)]) < 41 and float(Lats[Longs.index(lon)]) > 37:
           return "INSIDE"
    return "OUTSIDE"

udf_find_route_path = udf(find_route_path, StringType())

scaling_df = scaling_df.withColumn("IS_IN_AREA", udf_find_route_path("ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", "DEST_LATITUDE", "DEST_LONGITUDE", "DISTANCE"))

print("count after determining the routes that belongs to the target area: ", scaling_df.count())

#print("Writing csv file on hdfs ...")
#scaling_df.repartition(1).write.csv("")

print("Writing csv file on local machine ...")
scaling_df.repartition(1).write.csv("scale_model_2019_03_08_1720_1840", header = 'true', index=False)

print("Done scaling.")
