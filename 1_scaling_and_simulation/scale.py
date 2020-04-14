# scale_and_simulate_new.py
# To run this script, tray the following example
# python scale.py --start_datetime="2019-04-10 00:00:00" --end_datetime="2019-04-10 23:59:59" --output_folder="simulated-data-2019-04-10"

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

from_date = round(time.mktime(datetime.strptime(TARGET['start_datetime'], TARGET['date_pattern']).timetuple())/60)
to_date = round(time.mktime(datetime.strptime(TARGET['end_datetime'], TARGET['date_pattern']).timetuple())/60)

print("Reading data...")
scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/scaling_and_simulation/prepared_data/*.csv")
print("Remaining rows: ", scaling_df.count())

print("Filter in respect to day of month -> keeping yesterday, today and tomorrow... ")
wheels_on_dates = (TARGET['start_datetime'], "2020-01-02 18:00:00")
scaling_df = scaling_df.where(col('WHEELS_ON_UTC_DATETIME').between(*wheels_on_dates))

wheels_off_dates = ("2018-12-30 18:00:00", TARGET['end_datetime'])
scaling_df = scaling_df.where(col('WHEELS_OFF_UTC_DATETIME').between(*wheels_off_dates))
print("Remaining rows: ", scaling_df.count())

print("Calculating the flight position at each minute...")
dirpath = os.path.join('', TARGET['output_folder'])
if os.path.exists(dirpath) and os.path.isdir(dirpath):
    shutil.rmtree(dirpath)
os.mkdir(TARGET['output_folder'])

area_map = Basemap(llcrnrlon=-180, llcrnrlat=-90, urcrnrlon=180, urcrnrlat=90, lat_ts=0, resolution='l')

position_information = {}

route_information = {}

for current_time in range(from_date, to_date+1, 1):
    position_information[current_time] = []

increasing_row_id = 0
for row in scaling_df.rdd.collect():
    tail_number = row.TAIL_NUM
    flight_number = row.OP_CARRIER_FL_NUM
    flight_id = str(tail_number) + "-" + str(flight_number)
    origin_airport = row.ORIGIN
    destination_airport = row.DEST
    origin_city = row.ORIGIN_CITY_NAME
    dest_city = row.DEST_CITY_NAME
    origin_lon = row.ORIGIN_LONGITUDE
    origin_lat = row.ORIGIN_LATITUDE
    destination_lat = row.DEST_LATITUDE
    destination_lon = row.DEST_LONGITUDE
    wheels_off_utc_datetime = round(time.mktime(datetime.strptime(str(row.WHEELS_OFF_UTC_DATETIME), TARGET['date_pattern']).timetuple())/60)
    wheels_on_utc_datetime = round(time.mktime(datetime.strptime(str(row.WHEELS_ON_UTC_DATETIME), TARGET['date_pattern']).timetuple())/60)
    flight_start_time = wheels_off_utc_datetime
    flight_end_time = wheels_on_utc_datetime
    airtime_in_minutes = (flight_end_time - flight_start_time)
    
    if airtime_in_minutes < 10:
      continue
   
    increasing_row_id += 1
    Longs, Lats = area_map.gcpoints(origin_lon, origin_lat, destination_lon, destination_lat, airtime_in_minutes+1)

    route_details = {
        'tail_number': tail_number,
        'flight_number': flight_number, 
        'origin_lat': origin_lat, 
        'origin_lon': origin_lon, 
        'destination_lat': destination_lat, 
        'destination_lon': destination_lon,
        'origin_airport': origin_airport,
        'destination_airport': destination_airport,
        'origin_city': origin_city,
        'destination_city': dest_city,
        'origin_state': row.ORIGIN_STATE_NM,
        'destination_state': row.DEST_STATE_NM,
        'airtime_in_minutes': airtime_in_minutes,
        'distance_in_miles': row.DISTANCE,
        'wheels_off_utc_datetime': datetime.strptime(str(row.WHEELS_OFF_UTC_DATETIME), TARGET['date_pattern']).timetuple(),
        'wheels_on_utc_datetime': datetime.strptime(str(row.WHEELS_ON_UTC_DATETIME), TARGET['date_pattern']).timetuple()
    }

    route_information[increasing_row_id] = route_details

    for current_time in range(flight_start_time, flight_end_time+1, 1):
        if current_time >= from_date and current_time <= to_date:
            current_position = {
               'flight_id': increasing_row_id,
               'longitude': Longs[current_time-flight_start_time], 
               'latitude':  Lats[current_time-flight_start_time]
            }
            position_information[current_time].append(current_position)

print("Remaining rows: ", str(increasing_row_id))

print("Saving the route information...")
with open(TARGET['output_folder']+'/'+'route_information.json', 'w') as outfile:
    json.dump(route_information, outfile)

print("Saving the position_information...")
with open(TARGET['output_folder']+'/'+'position_information.json', 'w') as outfile:
    json.dump(position_information, outfile)

now = datetime.now()
print("Scaling finished at: ", now)
