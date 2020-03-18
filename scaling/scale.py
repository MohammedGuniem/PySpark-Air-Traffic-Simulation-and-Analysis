from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SQLContext, Row
from mpl_toolkits.basemap import Basemap
import time
import json

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

print("Started scaling ...")

scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/2019_03.csv")
scaling_df = scaling_df.fillna( { 'AIR_TIME':0 } )

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

def find_route_path(origin_lat, origin_lon, destination_lat, destination_lon, distance_in_miles, air_time_in_minutes, wheels_off):    
    area_map = Basemap(llcrnrlon=-109, llcrnrlat=37, urcrnrlon=-102, urcrnrlat=41, lat_ts=0, resolution='l')    
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat, (distance_in_miles/40)+1)
    
    inside_lon_array = []
    inside_lat_array = []
    time_array = []

    time_increase_rate = (air_time_in_minutes*60)/((distance_in_miles/40)+1)
    date_time = '2019-03-08 ' + str(wheels_off)
    pattern = '%Y-%m-%d %H%M'
    current_time = int(time.mktime(time.strptime(date_time, pattern)))

    for lon in Longs:
        if float(lon) < -102 and float(lon) > -109 and float(Lats[Longs.index(lon)]) < 41 and float(Lats[Longs.index(lon)]) > 37:
           inside_lon_array.append(str(lon))
           inside_lat_array.append(str(Lats[Longs.index(lon)]))
           time_array.append(current_time)
        current_time += time_increase_rate

    if len(inside_lon_array) == 0 or len(inside_lat_array) == 0:
       result = {'entry_lon': None, 'entry_lat': None, 'exit_lon': None, 'exit_lat': None, 'entry_time': None, 'exit_time': None, 'is_in_area': 'False'}
    else:    
       result = {'entry_lon': inside_lon_array[0],
                 'entry_lat': inside_lat_array[0],
                 'exit_lon': inside_lon_array[len(inside_lon_array)-1],
                 'exit_lat': inside_lat_array[len(inside_lat_array)-1],
                 'entry_time': str(time_array[0]),
                 'exit_time': str(time_array[len(time_array)-1]),
                 'is_in_area': 'True'}
    return json.dumps(result).replace(',','$')

udf_find_route_path = udf(find_route_path, StringType())

scaling_df = scaling_df.withColumn('ROUTE_PATH_INFO', udf_find_route_path("ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", "DEST_LATITUDE", "DEST_LONGITUDE", "DISTANCE", "AIR_TIME", "WHEELS_OFF"))

print("count after determining the routes that belongs to the target area: ", scaling_df.count())

#print("Writing csv file on hdfs ...")
#scaling_df.repartition(1).write.csv("")

print("Writing csv file on local machine ...")
scaling_df.repartition(1).write.csv("scale_model_2019_03_08_1720_1840", header = 'true')

print("Done scaling.")
