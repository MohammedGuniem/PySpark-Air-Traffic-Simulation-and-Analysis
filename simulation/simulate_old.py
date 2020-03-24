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

TARGET = {
 'year': '2019',
 'month': '05',
 'day': '25',
 'from_utc_time': '00:00:00',
 'to_utc_time': '23:59:59',
 'output_filename': 'air_traffic_25_mai_2019'
}

now = datetime.now()
print(now)

print("Started scaling ...")
scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/2019_05.csv")
print("Count of rows: ", scaling_df.count())

print("Filter in respect to day of month -> keeping yesterday, today and tomorrow... ")
scaling_df = scaling_df.filter(scaling_df.DAY_OF_MONTH.between(int(TARGET['day'])-1, int(TARGET['day'])+1))
print("Count of rows: ", scaling_df.count())

print("Removing flights that never been in the sky...")
scaling_df = scaling_df.na.drop(subset=["AIR_TIME"])
print("Count of rows: ", scaling_df.count())

print("Removing flights that never took off...")
scaling_df = scaling_df.filter(scaling_df.WHEELS_OFF.between(0000, 2359))
print("Count of rows: ", scaling_df.count())

print("Removing flights that never landed...")
scaling_df = scaling_df.filter(scaling_df.WHEELS_ON.between(0000, 2359))
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

#debug_1
#routes_df.repartition(1).write.csv("debug_1", header = 'true')

print("Joining the scaling dataframe with the routes information dataframe...")
scaling_df = scaling_df.join(routes_df.select(*["ROUTE_PATH",'ORIGIN_LATITUDE','ORIGIN_LONGITUDE','ORIGIN_UTC_LOCAL_TIME_VARIATION','DEST_LATITUDE','DEST_LONGITUDE','DEST_UTC_LOCAL_TIME_VARIATION']), "ROUTE_PATH")
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

#debug 2
#scaling_df.repartition(1).write.csv("debug_2", header = 'true')
scaling_df.repartition(1).write.csv("air_traffic_25_mai_2019_debug_2", header = 'true')

now = datetime.now()
print(now)


"""
print("testing map function")
f = scaling_df.collect()
for i in range(0,len(f),1):
     print(i)
     print(f[i].ROUTE_PATH)

def customFunction(row):
    return (row.ROUTE_PATH, row.ORIGIN_LATITUDE, row.ORIGIN_LONGITUDE)

sample2 = scaling_df.rdd.map(customFunction)
print(sample2)
"""
"""
print("Calculating the entry and exit points along with utc time for each of them and if the route passes through the area or not...")
schema = StructType([
    StructField("ROUTE_PATH", StringType(), False),
    StructField("ENTRY_LONGITUDE", DoubleType(), False),
    StructField("ENTRY_LATITUDE", DoubleType(), False),
    StructField("EXIT_LONGITUDE", DoubleType(), False),
    StructField("EXIT_LATITUDE", DoubleType(), False),
    StructField("ENTRY_TIME", DoubleType(), False),
    StructField("EXIT_TIME", DoubleType(), False),
    StructField("IS_IN_AREA", StringType(), False)
])

def find_route_path(route_path, origin_lat, origin_lon, destination_lat, destination_lon, distance_in_miles, air_time_in_minutes, wheels_off_utc_datetime):
    area_map = Basemap(llcrnrlon=-109, llcrnrlat=37, urcrnrlon=-102, urcrnrlat=41, lat_ts=0, resolution='l')
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat,3)#(distance_in_miles/40)+1)
    
    inside_lon_array = []
    inside_lat_array = []
    time_array = []

    time_increase_rate = (air_time_in_minutes*60)/((distance_in_miles/40)+1)
    start_time = datetime.strptime(str(wheels_off_utc_datetime), '%Y-%m-%d %H:%M:%S')
    current_time = int(start_time.strftime("%s"))

    for lon in Longs:
        if float(lon) < -102 and float(lon) > -109 and float(Lats[Longs.index(lon)]) < 41 and float(Lats[Longs.index(lon)]) > 37:
           inside_lon_array.append(lon)
           inside_lat_array.append(Lats[Longs.index(lon)])
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
    return (route_path, result['entry_lon'], result['entry_lat'], result['exit_lon'], result['exit_lat'], result['entry_time'], result['exit_time'], result['is_in_area'])

udf_find_route_path = udf(find_route_path, schema)

route_info_df = scaling_df.select(udf_find_route_path("ROUTE_PATH", "ORIGIN_LATITUDE", "ORIGIN_LONGITUDE", "DEST_LATITUDE", "DEST_LONGITUDE", "DISTANCE", "AIR_TIME", "WHEELS_OFF_UTC_DATETIME").alias("info"))

route_info_df = route_info_df.select("info.ROUTE_PATH", "info.ENTRY_LONGITUDE", "info.ENTRY_LATITUDE", "info.EXIT_LONGITUDE", "info.EXIT_LATITUDE", "info.ENTRY_TIME", "info.EXIT_TIME", "info.IS_IN_AREA")

print("Writing csv file on local machine ...")
route_info_df.repartition(1).write.csv("debug_3", header = 'true')
print("Count of rows: ", route_info_df.count())

print("Joining the scaling dataframe with the route information dataframe...")
scaling_df = scaling_df.join(route_info_df.select(*['ROUTE_PATH', "ENTRY_LONGITUDE", "ENTRY_LATITUDE", "EXIT_LONGITUDE", "EXIT_LATITUDE", "ENTRY_TIME", "EXIT_TIME", "IS_IN_AREA"]), "ROUTE_PATH")
print("Count of rows: ", scaling_df.count())

print("Writing csv file on local machine ...")
scaling_df.repartition(1).write.csv("debug_4", header = 'true')
"""
