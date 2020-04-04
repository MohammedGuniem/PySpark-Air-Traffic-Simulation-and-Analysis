# scale_and_simulate_new.py
# To run this script, tray the following example
# python scale_and_simulate_new.py --start_datetime="2019-05-25 00:00:00" --end_datetime="2019-05-25 23:59:59" --output_folder="simulated_data/"

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import concat_ws, mean as _mean, max as _max, desc, udf, col, lit
from pyspark.sql.types import StringType, TimestampType, StructType, StructField, DoubleType
from mpl_toolkits.basemap import Basemap
from datetime import datetime, timedelta
import argparse, sys
import time
import json

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
 'output_folder': args.output_filename #'simulated_data'
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
box_number = 1
print("box nr. ", box_number)

for llcrnrlon in range(-130, -60, 10):
    urcrnrlon = llcrnrlon+10
    for llcrnrlat in range(25, 50, 5):
        urcrnrlat = llcrnrlat+5
        all_routes = []
        area_map = Basemap(llcrnrlon=llcrnrlon, llcrnrlat=llcrnrlat, urcrnrlon=urcrnrlon, urcrnrlat=urcrnrlat, lat_ts=0, resolution='l')
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

                if LON < urcrnrlon and LON > llcrnrlon and LAT < urcrnrlat and LAT > llcrnrlat:
                    inside_lon_array.append(LON)
                    inside_lat_array.append(LAT)
                    time_array.append(current_time)
                current_time += time_increase_rate

            if (len(inside_lon_array) == 0 or len(inside_lat_array) == 0 or len(time_array) == 0):
                result = {'origin_lat': origin_lat, 'origin_lon': origin_lon, 'dest_lat': destination_lat, 'dest_lon': destination_lon, 'is_in_area': 'OUTSIDE'}
            elif (time_array[0] < from_date and time_array[len(time_array)-1] < from_date) or (time_array[0] > to_date and time_array[len(time_array)-1] > to_date):
                result = {'origin_lat': origin_lat, 'origin_lon': origin_lon, 'dest_lat': destination_lat, 'dest_lon': destination_lon, 'is_in_area': 'OUTSIDE'}
            else:
                result = {'origin_lat': origin_lat, 'origin_lon': origin_lon, 'dest_lat': destination_lat, 'dest_lon': destination_lon,
                        'entry_lon': inside_lon_array[0],
                        'entry_lat': inside_lat_array[0],
                        'exit_lon': inside_lon_array[len(inside_lon_array)-1],
                        'exit_lat': inside_lat_array[len(inside_lat_array)-1],
                        'entry_time': time_array[0],
                        'exit_time': time_array[len(time_array)-1],
                        'is_in_area': 'INSIDE'}
                result['route'] = row.ROUTE_PATH
                all_routes.append(result)

        print("Number of processed routes: ", len(all_routes))

        with open('/' + TARGET['output_folder'] + '/' + llcrnrlon + "_" + llcrnrlat + "_" + urcrnrlon + "_" + urcrnrlat +'.json', 'w') as outfile:
            json.dump(all_routes, outfile)

        box_number += 1

now = datetime.now()
print("Simulation finished at: ", now)