from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from datetime import datetime

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

now = datetime.now()
print("Started at: ", now)

print("Reading data...")
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/*.csv")
print("Count of rows: ", df.count())

print("Replacing empty delays with 0...")
df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )
print("Count of rows: ", df.count())

print("Calculating the total delay...")
df = df.withColumn("TOTAL_DELAY", col("CARRIER_DELAY") + col("WEATHER_DELAY") + col("NAS_DELAY") + col("SECURITY_DELAY") + col("LATE_AIRCRAFT_DELAY"))
print("Count of rows: ", df.count())

print("Categorizing delays...")
def categorize_delays(delay):
    if delay > 0 and delay <= 10:
       return "A"
    elif delay > 10 and delay <= 20:
       return "B"
    elif delay > 20 and delay <= 30:
       return "C"
    elif delay > 30 and delay <= 40:
       return "D"
    elif delay > 40 and delay <= 50:
       return "E"
    elif delay > 50 and delay <= 60:
       return "F"
    elif delay > 60:
       return "G"
    return "No delay"

categorize_delay_udf = udf(categorize_delays, StringType())

df = df.withColumn("CATEGORY_OF_CARRIER_DELAY", categorize_delay_udf(df["CARRIER_DELAY"]))
df = df.withColumn("CATEGORY_OF_WEATHER_DELAY", categorize_delay_udf(df["WEATHER_DELAY"]))
df = df.withColumn("CATEGORY_OF_NAS_DELAY", categorize_delay_udf(df["NAS_DELAY"]))
df = df.withColumn("CATEGORY_OF_SECURITY_DELAY", categorize_delay_udf(df["SECURITY_DELAY"]))
df = df.withColumn("CATEGORY_OF_LATE_AIRCRAFT_DELAY", categorize_delay_udf(df["LATE_AIRCRAFT_DELAY"]))
df = df.withColumn("CATEGORY_OF_TOTAL_DELAY", categorize_delay_udf(df["TOTAL_DELAY"]))
print("Count of rows: ", df.count())

print("Writing categorized data to the HDFS filesystem..")
df.repartition(1).write.csv("hdfs://master:9000/machine_learning/dataset_with_categorized_delays", header = 'true')

print("Writing csv file on local machine...")
df.repartition(1).write.csv("dataset_with_categorized_delays", header = 'true')

now = datetime.now()
print("Finished at: ", now)
