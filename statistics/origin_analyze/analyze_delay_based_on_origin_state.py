from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL to analyze delays based on origin state irrespective of route and destination") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel('FATAL')

df = spark.read.csv("data/2019_01.csv",header=True,sep=",")

df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

showRowCount = 10

origin_delay_df = df.groupBy('ORIGIN_STATE_NM')

print("Weather delay")
origin_weather_delay = (origin_delay_df.agg(_sum('WEATHER_DELAY').alias("1"),_mean('WEATHER_DELAY').alias('2')).sort(desc("1"))).toDF('Origin State','Total Sum','Average')
origin_weather_delay.show(showRowCount, False)

print("Carrier delay")
origin_carrier_delay = (origin_delay_df.agg(_sum('CARRIER_DELAY').alias("1"),_mean('CARRIER_DELAY').alias('2')).sort(desc("1"))).toDF('Origin State','Total Sum','Average')
origin_carrier_delay.show(showRowCount, False)

print("NAS delay")
origin_nas_delay = (origin_delay_df.agg(_sum('NAS_DELAY').alias("1"),_mean('NAS_DELAY').alias('2')).sort(desc("1"))).toDF('Origin State','Total Sum','Average')
origin_nas_delay.show(showRowCount, False)

print("Security delay")
origin_security_delay = (origin_delay_df.agg(_sum('SECURITY_DELAY').alias("1"),_mean('SECURITY_DELAY').alias('2')).sort(desc("1"))).toDF('Origin State','Total Sum','Average')
origin_security_delay.show(showRowCount, False)

print("Late aircraft delay")
origin_late_aircraft_delay = (origin_delay_df.agg(_sum('LATE_AIRCRAFT_DELAY').alias("1"),_mean('LATE_AIRCRAFT_DELAY').alias('2')).sort(desc("1"))).toDF('Origin State','Total Sum','Average')
origin_late_aircraft_delay.show(showRowCount, False)

print("The total number of states in origin is: " + str(origin_weather_delay.count()))
