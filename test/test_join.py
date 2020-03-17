from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, struct

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel('FATAL')

df1 = spark.read.csv("data/test2.csv",header=True,sep=",")
df1.show()

df2 = spark.read.csv("data/test3.csv",header=True,sep=",")
df2.show()


df_joined = df1.join(df2, "A")
df_joined.show()
