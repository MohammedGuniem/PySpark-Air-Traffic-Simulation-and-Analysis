from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL analyse delay") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel('FATAL')

df = spark.read.csv("data/2019_01.csv",header=True,sep=",")
df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

rowCount = (df.groupBy('DEST_STATE_NM').agg(_mean('WEATHER_DELAY').alias('Average weather delay per destination state in minutes'))).count()

dest_delay_df = df.groupBy('DEST_STATE_NM')

#(dest_delay_df.agg(_sum('WEATHER_DELAY').alias("1"),_mean('WEATHER_DELAY').alias('2'),_sum('CARRIER_DELAY').alias("3"),_mean('CARRIER_DELAY').alias('4')).sort(asc("DEST_STATE_NM"))).toDF('Destination state','total sum of weather delay in minutes','avg sum of weather delay in minutes','total sum of carrier delay in minutes','avg sum of carrier delay in minutes').show(rowCount, False)

(dest_delay_df.agg(_sum('WEATHER_DELAY').alias("1"),_mean('WEATHER_DELAY').alias('2')).sort(desc("1"))).toDF('Destination state','total sum of weather delay in minutes','avg sum of weather delay in minutes').show(rowCount, False)

(dest_delay_df.agg(_sum('CARRIER_DELAY').alias("1"),_mean('CARRIER_DELAY').alias('2')).sort(desc("1"))).toDF('Destination state','total sum of carrier delay in minutes','avg sum of carrier delay in minutes').show(rowCount, False)

(dest_delay_df.agg(_sum('NAS_DELAY').alias("1"),_mean('NAS_DELAY').alias('2')).sort(desc("1"))).toDF('Destination state','total sum of NAS delay in minutes','avg sum of NAS delay in minutes').show(rowCount, False)

(dest_delay_df.agg(_sum('SECURITY_DELAY').alias("1"),_mean('SECURITY_DELAY').alias('2')).sort(desc("1"))).toDF('Destination state','total sum of security delay in minutes','avg sum of security delay in minutes').show(rowCount, False)

(dest_delay_df.agg(_sum('LATE_AIRCRAFT_DELAY').alias("1"),_mean('LATE_AIRCRAFT_DELAY').alias('2')).sort(desc("1"))).toDF('Destination state','total sum of late aircraft delay in minutes','avg sum of late aircraft delay in minutes').show(rowCount, False)

print("The row count for weather delay is: ", rowCount)
