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
 'start_datetime': '2019-05-25 00:00:00',
 'end_datetime': '2019-05-25 23:59:59',
 'date_pattern': '%Y-%m-%d %H:%M:%S',
 'output_filename': 'scaled_data'
}


now = datetime.now()
print("Started scaling at: ", now)

scaling_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/scaling_and_simulation/prepared_data/part*.csv")
print("Count of rows: ", scaling_df.count())

print("Filter in respect to day of month -> keeping yesterday, today and tomorrow... ")
wheels_on_dates = (TARGET['start_datetime'], "2020-01-02 18:00:00")
scaling_df = scaling_df.where(col('WHEELS_ON_UTC_DATETIME').between(*wheels_on_dates))
print("Count of rows: ", scaling_df.count())

wheels_off_dates = ("2018-12-30 18:00:00", TARGET['end_datetime'])
scaling_df = scaling_df.where(col('WHEELS_OFF_UTC_DATETIME').between(*wheels_off_dates))
print("Count of rows: ", scaling_df.count())

scaling_df.repartition(1).write.csv(TARGET['output_filename'], header = 'true')

now = datetime.now()
print("Finished scaling at: ", now)
