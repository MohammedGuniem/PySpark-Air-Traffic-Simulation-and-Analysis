from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, struct

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL analyse delay by route from origin airport to destination airport") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel('FATAL')

df = spark.read.csv("data/2019_01.csv",header=True,sep=",")

df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

route_delay_df = df.withColumn("Route", struct(col("ORIGIN"), col("DEST"))).groupBy('Route')

showRowCount = 10

weather_delay_df = (route_delay_df.agg(_sum('WEATHER_DELAY').alias("1"),_mean('WEATHER_DELAY').alias('2')).sort(desc("1"))).toDF('Route [From state, To state]','Sum','average')
print("Weather delay in minutes")
weather_delay_df.show(showRowCount, False)

carrier_delay_df = (route_delay_df.agg(_sum('CARRIER_DELAY').alias("1"),_mean('CARRIER_DELAY').alias('2')).sort(desc("1"))).toDF('Route [From state, To state]','Sum','average')
print("Carrier delay in minutes")
carrier_delay_df.show(showRowCount, False)

nas_delay_df = (route_delay_df.agg(_sum('NAS_DELAY').alias("1"),_mean('NAS_DELAY').alias('2')).sort(desc("1"))).toDF('Route [From state, To state]','Sum','average')
print("NAS delay in minutes")
nas_delay_df.show(showRowCount, False)

security_delay_df = (route_delay_df.agg(_sum('SECURITY_DELAY').alias("1"),_mean('SECURITY_DELAY').alias('2')).sort(desc("1"))).toDF('Route [From state, To state]','Sum','average')
print("Security delay in minutes")
security_delay_df.show(showRowCount, False)

late_aircraft_delay_df = (route_delay_df.agg(_sum('LATE_AIRCRAFT_DELAY').alias("1"),_mean('LATE_AIRCRAFT_DELAY').alias('2')).sort(desc("1"))).toDF('Route [From state, To state]','Sum','average')
print("Late aircraft delay in minutes")
late_aircraft_delay_df.show(showRowCount, False)

totalNumberOfUniqueRoutes = weather_delay_df.count()
print("The total number of unique routes is: " + str(totalNumberOfUniqueRoutes))
