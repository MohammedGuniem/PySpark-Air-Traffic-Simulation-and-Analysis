from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, udf, lit
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql import SQLContext, Row
from mpl_toolkits.basemap import Basemap
import time
import json

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

print("Started scaling ...")

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/2019_03.csv")
df = df.withColumn("DATE", df["FL_DATE"].cast(StringType()))

print("Number of rows before date filter", df.count())

df = df.filter(df.FL_DATE == "2019-03-08")

print("Number of rows after date filter", df.count())

print("Number of rows before removing flights that has never took off in dataframe: ", df.count())

df = df.filter(df.WHEELS_OFF.between(0000, 2359))

print("Number of rows aftter removing flights that has never took off in dataframe: ", df.count())

airports_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/support_data/airports_data.csv")

airports_df = airports_df.withColumnRenamed('AIRPORT_SEQ_ID','ORIGIN_AIRPORT_SEQ_ID')
airports_df = airports_df.withColumn("DEST_AIRPORT_SEQ_ID", airports_df["ORIGIN_AIRPORT_SEQ_ID"])

print("total number of data row before joining with coordinates: ", df.count()) 

df = df.join(airports_df.select(*["ORIGIN_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE"]), "ORIGIN_AIRPORT_SEQ_ID")
df = df.withColumnRenamed('LATITUDE','ORIGIN_LATITUDE')
df = df.withColumnRenamed('LONGITUDE','ORIGIN_LONGITUDE')

df = df.join(airports_df.select(*["DEST_AIRPORT_SEQ_ID","LATITUDE","LONGITUDE"]), "DEST_AIRPORT_SEQ_ID")
df = df.withColumnRenamed('LATITUDE','DEST_LATITUDE')
df = df.withColumnRenamed('LONGITUDE','DEST_LONGITUDE')

print("total number of data row after joining with coordinates: ", df.count())

df = df.select([c for c in df.columns if c in ["DATE", "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", "DEST_LATITUDE", "DEST_LONGITUDE", "DISTANCE", "AIR_TIME", "WHEELS_OFF"]])
df = df.fillna( { 'AIR_TIME':0 } )

schema = StructType([
    StructField("DATE", StringType(), False),
    StructField("ORIGIN_LONGITUDE", StringType(), False),
    StructField("ORIGIN_LATITUDE", StringType(), False),
    StructField("DEST_LONGITUDE", StringType(), False),
    StructField("DEST_LATITUDE", StringType(), False),
    StructField("DISTANCE", StringType(), False),
    StructField("AIR_TIME", StringType(), False),
    StructField("WHEELS_OFF", StringType(), False),
    StructField("ENTRY_LONGITUDE", StringType(), False),
    StructField("ENTRY_LATITUDE", StringType(), False),
    StructField("EXIT_LONGITUDE", StringType(), False),
    StructField("EXIT_LATITUDE", StringType(), False),
    StructField("ENTRY_TIME", StringType(), False),
    StructField("EXIT_TIME", StringType(), False),
    StructField("IS_IN_AREA", StringType(), False)
])

def find_route_path(date, origin_lat, origin_lon, destination_lat, destination_lon, distance_in_miles, air_time_in_minutes, wheels_off):
    area_map = Basemap(llcrnrlon=-109, llcrnrlat=37, urcrnrlon=-102, urcrnrlat=41, lat_ts=0, resolution='l')    
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat, (distance_in_miles/40)+1)
    
    inside_lon_array = []
    inside_lat_array = []
    time_array = []

    time_increase_rate = (air_time_in_minutes*60)/((distance_in_miles/40)+1)
    date = str(date).split(" ")[0]
    date_time = date + ' ' + str(wheels_off)

    digit_count = len(str(wheels_off))

    if digit_count == 1:
        wheels_off = "000"+str(wheels_off)
    elif digit_count == 2:
        wheels_off = "00"+str(wheels_off)
    elif digit_count == 3:
        wheels_off = "0"+str(wheels_off)
    else:
        wheels_off = str(wheels_off)
    pattern = '%Y-%m-%d %H%M'
    date_time = date + ' ' + str(wheels_off)
    current_time = int(time.mktime(time.strptime(date_time, pattern)))

    for lon in Longs:
        if float(lon) < -102 and float(lon) > -109 and float(Lats[Longs.index(lon)]) < 41 and float(Lats[Longs.index(lon)]) > 37:
           inside_lon_array.append(str(lon))
           inside_lat_array.append(str(Lats[Longs.index(lon)]))
           time_array.append(current_time)
        current_time += time_increase_rate

    if len(inside_lon_array) == 0 or len(inside_lat_array) == 0:
       result = {'entry_lon': 'null', 'entry_lat': 'null', 'exit_lon': 'null', 'exit_lat': 'null', 'entry_time': 'null', 'exit_time': 'null', 'is_in_area': 'OUTSIDE'}
    else:    
       result = {'entry_lon': inside_lon_array[0],
                 'entry_lat': inside_lat_array[0],
                 'exit_lon': inside_lon_array[len(inside_lon_array)-1],
                 'exit_lat': inside_lat_array[len(inside_lat_array)-1],
                 'entry_time': str(time_array[0]),
                 'exit_time': str(time_array[len(time_array)-1]),
                 'is_in_area': 'INSIDE'}
    return (str(date), str(origin_lat), str(origin_lon), str(destination_lat), str(destination_lon), str(distance_in_miles), str(air_time_in_minutes), str(wheels_off), result['entry_lon'], result['entry_lat'], result['exit_lon'], result['exit_lat'], result['entry_time'], result['exit_time'], result['is_in_area'])

udf_find_route_path = udf(find_route_path, schema)

route_info_df = df.select(udf_find_route_path("DATE", "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", "DEST_LATITUDE", "DEST_LONGITUDE", "DISTANCE", "AIR_TIME", "WHEELS_OFF").alias("info"))

route_info_df = route_info_df.select("info.DATE", "info.ORIGIN_LATITUDE", "info.ORIGIN_LONGITUDE", "info.DEST_LATITUDE", "info.DEST_LONGITUDE", "info.DISTANCE", "info.AIR_TIME", "info.WHEELS_OFF", "info.ENTRY_LONGITUDE", "info.ENTRY_LATITUDE", "info.EXIT_LONGITUDE", "info.EXIT_LATITUDE", "info.ENTRY_TIME", "info.EXIT_TIME", "info.IS_IN_AREA")

#print("Writing csv file on hdfs ...")
#scaling_df.repartition(1).write.csv("")

print("Writing csv file on local machine ...")
route_info_df.repartition(1).write.csv("route_information_over_colorado_in_mars", header = 'true')

#print("Done scaling.")
