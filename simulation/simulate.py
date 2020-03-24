from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws, mean as _mean, max as _max, desc, udf, col, lit
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, DoubleType
from datetime import datetime, timedelta
import time
from mpl_toolkits.basemap import Basemap
import json

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

now = datetime.now()
print("Started scaling at: ", now)

print("Reading data...")
scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("scaled_data/part-*.csv") #"hdfs://master:9000/datas/.csv")
print("Count of rows: ", scaling_df.count())

print("Constructing the route paths in one column...")
scaling_df = scaling_df.withColumn("ROUTE_PATH", concat_ws("->", "ORIGIN_AIRPORT_SEQ_ID", "DEST_AIRPORT_SEQ_ID"))
print("Count of rows: ", scaling_df.count())

print("Calculating the entry and exit points along with utc time for each of them and if the route passes through the area or not...")
all_routes = []
area_map = Basemap(llcrnrlon=-109, llcrnrlat=37, urcrnrlon=-102, urcrnrlat=41, lat_ts=0, resolution='l')
for row in scaling_df.rdd.collect():
    origin_lon = row.ORIGIN_LONGITUDE
    origin_lat = row.ORIGIN_LATITUDE
    destination_lat = row.DEST_LATITUDE
    destination_lon = row.DEST_LONGITUDE
    distance_in_miles = row.DISTANCE
    air_time_in_minutes = row.AIR_TIME
    wheels_off_utc_datetime = row.WHEELS_OFF_UTC_DATETIME
    
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat,(distance_in_miles/40)+1)
    inside_lon_array = []
    inside_lat_array = []
    time_array = []

    time_increase_rate = (air_time_in_minutes*60)/((distance_in_miles/40)+1)
    start_time = datetime.strptime(str(wheels_off_utc_datetime), '%Y-%m-%d %H:%M:%S')
    current_time = int(start_time.strftime("%s"))

    for i in range(len(Longs)):
        LAT = float(Lats[i])
        LON = float(Longs[i])
        
        if LON < -102 and LON > -109 and LAT < 41 and LAT > 37:
           inside_lon_array.append(LON)
           inside_lat_array.append(LAT)
           time_array.append(current_time)
        current_time += time_increase_rate

    if len(inside_lon_array) == 0 or len(inside_lat_array) == 0:
       result = {'origin_lat': origin_lat, 'origin_lon': origin_lon, 'dest_lat': destination_lat, 'dest_lon': destination_lon, 'is_in_area': 'OUTSIDE'}
    else:    
       result = {'origin_lat': origin_lat, 'origin_lon': origin_lon, 'dest_lat': destination_lat, 'dest_lon': destination_lon,
                 'entry_lon': inside_lon_array[0],
                 'entry_lat': inside_lat_array[0],
                 'exit_lon': inside_lon_array[len(inside_lon_array)-1],
                 'exit_lat': inside_lat_array[len(inside_lat_array)-1],
                 'entry_time': str(time_array[0]),
                 'exit_time': str(time_array[len(time_array)-1]),
                 'is_in_area': 'INSIDE'}
    result['route'] = row.ROUTE_PATH
    all_routes.append(result)

print("Number of processed routes: ", len(all_routes))

with open('simulated_data.json', 'w') as outfile:
    json.dump(all_routes, outfile)

now = datetime.now()
print("Finished scaling at: ", now)
