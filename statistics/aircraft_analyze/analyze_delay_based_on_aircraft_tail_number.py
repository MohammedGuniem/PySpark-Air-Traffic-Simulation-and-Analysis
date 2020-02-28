from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL to analyze delays based on aircraft irrespective of route, origin or destination") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel('FATAL')

df = spark.read.csv("data/2019_01.csv",header=True,sep=",")

df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

showRowCount = 10

aircraft_delay_df = df.groupBy('TAIL_NUM')

print("Weather delay")
aircraft_weather_delay = (aircraft_delay_df.agg(_sum('WEATHER_DELAY').alias("1"),_mean('WEATHER_DELAY').alias('2')).sort(desc("1"))).toDF('Aircraft Tail Number','Total Sum','Average')
aircraft_weather_delay.show(showRowCount, False)

print("Carrier delay")
aircraft_carrier_delay = (aircraft_delay_df.agg(_sum('CARRIER_DELAY').alias("1"),_mean('CARRIER_DELAY').alias('2')).sort(desc("1"))).toDF('Aircraft Tail Number','Total Sum','Average')
aircraft_carrier_delay.show(showRowCount, False)

print("NAS delay")
aircraft_nas_delay = (aircraft_delay_df.agg(_sum('NAS_DELAY').alias("1"),_mean('NAS_DELAY').alias('2')).sort(desc("1"))).toDF('Aircraft Tail Number','Total Sum','Average')
aircraft_nas_delay.show(showRowCount, False)

print("Security delay")
aircraft_security_delay = (aircraft_delay_df.agg(_sum('SECURITY_DELAY').alias("1"),_mean('SECURITY_DELAY').alias('2')).sort(desc("1"))).toDF('Aircraft Tail Number','Total Sum','Average')
aircraft_security_delay.show(showRowCount, False)

print("Late aircraft delay")
aircraft_late_aircraft_delay = (aircraft_delay_df.agg(_sum('LATE_AIRCRAFT_DELAY').alias("1"),_mean('LATE_AIRCRAFT_DELAY').alias('2')).sort(desc("1"))).toDF('Aircraft Tail Number','Total Sum','Average')
aircraft_late_aircraft_delay.show(showRowCount, False)

print("The total number of aircrafts in origin is: " + str(aircraft_weather_delay.count()))
