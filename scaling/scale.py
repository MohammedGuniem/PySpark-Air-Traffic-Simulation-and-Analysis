from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws
from pyspark.sql.types import StringType
from pyspark.sql import SQLContext
from mpl_toolkits.basemap import Basemap

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/2019_03.csv")

showRowCount = 10 

print("Started scaling ...")

from pyspark.sql.functions import *

print("Initial number of rows in dataframe: ", df.count())

scaling_df = df.filter(df.FL_DATE == "2019-03-08")

print("count after filtering in respect to date: ", scaling_df.count()) 

scaling_df = scaling_df.filter(scaling_df.WHEELS_OFF.between(1720, 1840))

scaling_df = scaling_df.filter(scaling_df.WHEELS_ON.between(1720,1840))

print("count after filtering between departure time and arrival time: ", scaling_df.count())

airports_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/support_data/airports_data.csv")
airports_df = airports_df.toPandas().groupby('AIRPORT_SEQ_ID')
global airports_data
airports_data = {}
for key, grp in airports_df:
    airports_data[key] = grp.to_dict('records')

def find_route_path(origin_airport_seq_id, destination_airport_seq_id, distance_in_miles):    
    area_map = Basemap(llcrnrlon=-109, llcrnrlat=37, urcrnrlon=-102, urcrnrlat=41, lat_ts=0, resolution='l')    
    origin_lat = airports_data[origin_airport_seq_id][0]["LATITUDE"]
    origin_lon = airports_data[origin_airport_seq_id][0]['LONGITUDE']
    destination_lat = airports_data[destination_airport_seq_id][0]["LATITUDE"]
    destination_lon = airports_data[destination_airport_seq_id][0]['LONGITUDE']
   
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat, (distance_in_miles/40)+1)
    for lon in Longs:
        if float(lon) < -102 and float(lon) > -109 and float(Lats[Longs.index(lon)]) < 41 and float(Lats[Longs.index(lon)]) > 37:
           return str(origin_lat) + "|" + str(origin_lon) + "|" + str(destination_lat) + "|" + str(destination_lon) + "|INSIDE"
    return str(origin_lat) + "|" + str(origin_lon) + "|" + str(destination_lat) + "|" + str(destination_lon) + "|OUTSIDE"

udf_find_route_path = udf(find_route_path, StringType())

scaling_df = scaling_df.withColumn("ROUTE_PATH", udf_find_route_path("ORIGIN_AIRPORT_SEQ_ID", "DEST_AIRPORT_SEQ_ID", "DISTANCE"))

print("count after determining if route passes over Colorado or not: ", scaling_df.count())

#scaling_df.show(showRowCount, False)

#print("Writing csv file on hdfs ...")
#scaling_df.repartition(1).write.csv("scale_model_2019_03_08_1720_1840")

print("Writing csv file on local machine ...")
scaling_df.repartition(1).write.csv("scale_model_2019_03_08_1720_1840", header = 'true')

print("Done scaling.")
