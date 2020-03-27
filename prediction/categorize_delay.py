
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf,sum,when,col
from pyspark.sql.types import StringType
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from mpl_toolkits.basemap import Basemap

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

print("Categorizing the delays from dataset")

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/*.csv")

print("Categorizing the delays in dataset")

A = df[('CARRIER_DELAY','WEATHER_DELAY','NAS_DELAY','SECURITY_DELAY','LATE_AIRCRAFT_DELAY')].fillna(0)

B = A.withColumn('TOTAL', A.CARRIER_DELAY + A.WEATHER_DELAY + A.NAS_DELAY + A.SECURITY_DELAY + A.LATE_AIRCRAFT_DELAY)

def colToString(num):
    if num >= 0 and num < 6.0: return 'A'
    elif num >= 6.0 and num < 11.0: return 'B'
    elif num >= 11.0 and num < 16.0: return 'C'
    elif num >= 16.0 and num < 21.0: return 'D'
    else: return 'E'


myUdf = udf(colToString, StringType())

B.withColumn("category", myUdf('TOTAL')).show()

C = B.withColumn("STATUS", F.when(F.col('TOTAL') > 15, "Delayed").otherwise("Not Delayed"))

C.show()

C.groupBy("STATUS").count().show()





















 
