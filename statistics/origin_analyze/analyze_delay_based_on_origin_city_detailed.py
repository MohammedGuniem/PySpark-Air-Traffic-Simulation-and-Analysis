from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count
from pyspark.sql import SQLContext

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('hdfs://master:9000/dataset/*.csv')

df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

showRowCount = 10

origin_delay_df = df.groupBy('ORIGIN_CITY_NAME')

print("Weather delay")
origin_weather_delay = (origin_delay_df.agg(_sum('WEATHER_DELAY').alias("1"),_mean('WEATHER_DELAY').alias('2')).sort(desc("1"))).toDF('Origin City','Total Sum','Average')
origin_weather_delay.show(showRowCount, False)

print("Carrier delay")
origin_carrier_delay = (origin_delay_df.agg(_sum('CARRIER_DELAY').alias("1"),_mean('CARRIER_DELAY').alias('2')).sort(desc("1"))).toDF('Origin City','Total Sum','Average')
origin_carrier_delay.show(showRowCount, False)

print("NAS delay")
origin_nas_delay = (origin_delay_df.agg(_sum('NAS_DELAY').alias("1"),_mean('NAS_DELAY').alias('2')).sort(desc("1"))).toDF('Origin City','Total Sum','Average')
origin_nas_delay.show(showRowCount, False)

print("Security delay")
origin_security_delay = (origin_delay_df.agg(_sum('SECURITY_DELAY').alias("1"),_mean('SECURITY_DELAY').alias('2')).sort(desc("1"))).toDF('Origin City','Total Sum','Average')
origin_security_delay.show(showRowCount, False)

print("Late aircraft delay")
origin_late_aircraft_delay = (origin_delay_df.agg(_sum('LATE_AIRCRAFT_DELAY').alias("1"),_mean('LATE_AIRCRAFT_DELAY').alias('2')).sort(desc("1"))).toDF('Origin City','Total Sum','Average')
origin_late_aircraft_delay.show(showRowCount, False)

print("The total number of cities in origin is: " + str(origin_weather_delay.count()))
