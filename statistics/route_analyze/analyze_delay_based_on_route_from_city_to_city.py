from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws
from pyspark.sql import SQLContext

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('hdfs://master:9000/dataset/*.csv')

df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

showRowCount = 10

city_route_delay_df = df.withColumn("TOTAL_DELAY", col("WEATHER_DELAY")+col("CARRIER_DELAY")+col('NAS_DELAY')+col('SECURITY_DELAY')+col('LATE_AIRCRAFT_DELAY'))

city_route_delay_df = city_route_delay_df.withColumn("ROUTE_BETWEEN_CITIES", concat_ws("->", 'ORIGIN_CITY_NAME', 'DEST_CITY_NAME')).groupBy('ROUTE_BETWEEN_CITIES') 

city_route_delay_df = city_route_delay_df.agg(
		_sum('TOTAL_DELAY').alias('SUM_OF_ALL_DELAYS'),_mean('TOTAL_DELAY').alias('AVERAGE_OF_ALL_DELAYS'),
		_sum('WEATHER_DELAY').alias("SUM_OF_WEATHER_DELAY"),_mean('WEATHER_DELAY').alias('AVERAGE_OF_WEATHER_DELAY'),
		_sum('CARRIER_DELAY').alias("SUM_OF_CARRIER_DELAY"),_mean('CARRIER_DELAY').alias('AVERAGE_OF_CARRIER_DELAY'),
		_sum('NAS_DELAY').alias("SUM_OF_NAS_DELAY"),_mean('NAS_DELAY').alias('AVERAGE_OF_NAS_DELAY'),
		_sum('SECURITY_DELAY').alias("SUM_OF_SECURITY_DELAY"),_mean('SECURITY_DELAY').alias('AVERAGE_OF_SECURITY_DELAY'),
		_sum('LATE_AIRCRAFT_DELAY').alias("SUM_OF_LATE_AIRCRAFT_DELAY"),_mean('LATE_AIRCRAFT_DELAY').alias('AVERAGE_OF_LATE_AIRCRAFT_DELAY'),
	).sort(desc("SUM_OF_ALL_DELAYS"))

city_route_delay_df.show(showRowCount, False)

print("The total number of routes between 2 cities is: " + str(city_route_delay_df.count()))

print("Writing to csv files on hdfs ...")

#aircraft_delay_df.repartition(1).write.csv('hdfs://master:9000/statistics/aircraft_analyze/analyze_aircraft_delay')

city_route_delay_df.repartition(1).write.csv('analyze_city_route_delay', header = 'true')

print("done")

