from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws
from pyspark.sql import SQLContext

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/*.csv")

showRowCount = 10 

print("Started scaling ...")

from pyspark.sql.functions import *

print("count before constructing date columns: ", df.count())
scaling_df = df.withColumn("DATE", concat_ws("-", "YEAR", "MONTH", "DAY_OF_MONTH"))

print("count after constructing date columns: ", scaling_df.count()) 

scaling_df = scaling_df.filter(scaling_df.DATE.isin("2019-3-8"))

print("count after filtering in respect to date: ", scaling_df.count()) 

scaling_df = scaling_df.filter(scaling_df.WHEELS_OFF.between(1720, 1840))

scaling_df = scaling_df.filter(scaling_df.WHEELS_ON.between(1720,1840))

print("count after filtering between departure time and arrival time: ", scaling_df.count())

scaling_df.show(showRowCount, False)

#print("Writing csv file on hdfs ...")
#delay_df.repartition(1).write.csv(hdfs_csv_writing_paths)

print("Writing csv file on local machine ...")
scaling_df.repartition(1).write.csv("scale_model_2019_03_08_1720_1840", header = 'true')

print("Done scaling.")
