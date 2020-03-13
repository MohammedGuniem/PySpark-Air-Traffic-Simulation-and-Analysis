from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import lit, col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws
from pyspark.sql import SQLContext

def squared(s):
  return s * s
SQLContext.udf.register("squaredWithPython", squared)


squared_udf = udf(squared, LongType())
df = SQLContext.table("test")
display(df.select("id", squared_udf("id").alias("id_squared")))
