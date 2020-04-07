# scale_and_simulate_new.py
# To run this script, tray the following example
# python scale_and_simulate_new2.py --start_datetime="2019-05-25 00:00:00" --end_datetime="2019-05-25 23:59:59" --output_folder="simulated_data"

for i in range(3,5+1,1):
    print(i)

import sys
sys.exit()
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws, mean as _mean, max as _max, desc, udf, col, lit
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, DoubleType
from mpl_toolkits.basemap import Basemap
from datetime import datetime, timedelta
import argparse, sys, json, time, os, shutil

# Scaling
now = datetime.now()
print("Scaling started scaling at: ", now)

parser = argparse.ArgumentParser()
parser.add_argument('--start_datetime', help='Enter the start datetime of your simulation in the following format %Y-%m-%d %H:%M:%S')
parser.add_argument('--end_datetime', help='Enter the end datetime of your simulation in the following format %Y-%m-%d %H:%M:%S')
parser.add_argument('--output_folder', help='Enter the name of your output file')
args = parser.parse_args()

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

TARGET = {
 'start_datetime': args.start_datetime, #'2019-05-25 00:00:00',
 'end_datetime': args.end_datetime, #'2019-05-25 23:59:59',
 'date_pattern': '%Y-%m-%d %H:%M:%S',
 'output_folder': args.output_folder #'simulated_data'
}

print("Reading data...")
scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/scaling_and_simulation/prepared_data/*.csv")
print("Remaining rows: ", scaling_df.count())

print("Filter in respect to day of month -> keeping yesterday, today and tomorrow... ")
wheels_on_dates = (TARGET['start_datetime'], "2020-01-02 18:00:00")
scaling_df = scaling_df.where(col('WHEELS_ON_UTC_DATETIME').between(*wheels_on_dates))
print("Remaining rows: ", scaling_df.count())

wheels_off_dates = ("2018-12-30 18:00:00", TARGET['end_datetime'])
scaling_df = scaling_df.where(col('WHEELS_OFF_UTC_DATETIME').between(*wheels_off_dates))
print("Remaining rows: ", scaling_df.count())

now = datetime.now()
print("Scaling finished at: ", now)

# Simulation
now = datetime.now()
print("Simulation started at: ", now)

from_date = time.mktime(datetime.strptime(TARGET['start_datetime'], TARGET['date_pattern']).timetuple())
to_date = time.mktime(datetime.strptime(TARGET['end_datetime'], TARGET['date_pattern']).timetuple())

print("Constructing the route paths in one column...")
scaling_df = scaling_df.withColumn("ROUTE_PATH", concat_ws("->", "ORIGIN_AIRPORT_SEQ_ID", "DEST_AIRPORT_SEQ_ID"))
print("Remaining rows: ", scaling_df.count())

print("Calculating the entry and exit points along with utc time for each of them and if the route passes through the area or not...")
dirpath = os.path.join('', TARGET['output_folder'])
if os.path.exists(dirpath) and os.path.isdir(dirpath):
    shutil.rmtree(dirpath)
os.mkdir(TARGET['output_folder'])

area_map = Basemap(llcrnrlon=-180, llcrnrlat=-90, urcrnrlon=180, urcrnrlat=90, lat_ts=0, resolution='l')

position_information = {}

route_information = {}

for current_time in range(from_date, to_date+1, 1):
    position_information[current_time] = []

route_scaling_id = 1

for row in scaling_df.rdd.collect():
    origin_airport = row.ORIGIN
    destination_airport = row.DEST
    origin_city = row.ORIGIN_CITY_NAME
    dest_city = row.DEST_CITY_NAME
    origin_lon = row.ORIGIN_LONGITUDE
    origin_lat = row.ORIGIN_LATITUDE
    destination_lat = row.DEST_LATITUDE
    destination_lon = row.DEST_LONGITUDE
    wheels_off_utc_datetime = row.WHEELS_OFF_UTC_DATETIME
    wheels_on_utc_datetime = row.WHEELS_ON_UTC_DATETIME
    flight_start_time = round(datetime.strptime(str(wheels_off_utc_datetime), '%Y-%m-%d %H:%M:%S'))
    flight_end_time = round(datetime.strptime(str(wheels_on_utc_datetime), '%Y-%m-%d %H:%M:%S'))
    airtime_in_minutes = (flight_end_time.strftime("%s") - flight_start_time.strftime("%s"))/60

    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat, airtime_in_minutes)
    
    route_details = {
        'origin_lat': origin_lat, 
        'origin_lon': origin_lon, 
        'destination_lat': destination_lat, 
        'destination_lon': destination_lon,
        'origin_airport': origin_airport,
        'destination_airport': destination_airport,
        'origin_city': origin_city,
        'dest_city': dest_city
    }

    route_information[route_scaling_id] = route_details

    for current_time in range(flight_start_time, flight_end_time+1, 1):
        current_position = {
            'route_scaling_id': route_scaling_id,
            'longitude': Longs[current_time-flight_start_time], 
            'latitude':  Lats[current_time-flight_start_time]
        }

        position_information[current_time].append(current_position)
    
    route_scaling_id += 1

print("Saving the route information")
with open(TARGET['output_folder']+'/'+'route_information.json', 'w') as outfile:
    json.dump(route_information, outfile)

print("Saving the position_information")
with open(TARGET['output_folder']+'/'+'position_information.json', 'w') as outfile:
    json.dump(position_information, outfile)

now = datetime.now()
print("Simulation finished at: ", now)
