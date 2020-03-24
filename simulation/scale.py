from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from datetime import datetime, timedelta
import time
from mpl_toolkits.basemap import Basemap

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

TARGET = {
 'from': {
    'year': '2019',
    'month': '05',
    'day': '25',
    'utc_time': '15:00:00'
 },
 'to': {
    'year': '2019',
    'month': '05',
    'day': '25',
    'utc_time': '15:10:00'
 },
 'output_filename': 'output'
}

now = datetime.now()
print("Started scaling at: ", now)

scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("prepared_data/part*.csv") #"hdfs://master:9000/dataset/prepared_data/part*.csv")
print("Count of rows: ", scaling_df.count())

print("Filter in respect to day of month -> keeping yesterday, today and tomorrow... ")
from_datetime = TARGET['from']['year']+"-"+TARGET['from']['month']+"-"+TARGET['from']['day']+" "+TARGET['from']['utc_time']
to_datetime =  TARGET['to']['year']+"-"+TARGET['to']['month']+"-"+TARGET['to']['day']+" "+TARGET['to']['utc_time']

wheels_on_dates = (from_datetime, "2020-01-02 18:00:00")
scaling_df = scaling_df.where(col('WHEELS_ON_UTC_DATETIME').between(*wheels_on_dates))
print("Count of rows: ", scaling_df.count())

wheels_off_dates = ("2018-12-30 18:00:00", to_datetime)
scaling_df = scaling_df.where(col('WHEELS_OFF_UTC_DATETIME').between(*wheels_off_dates))

print("Count of rows: ", scaling_df.count())

scaling_df.repartition(1).write.csv("scaled_data", header = 'true')

now = datetime.now()
print("Finished scaling at: ", now)
