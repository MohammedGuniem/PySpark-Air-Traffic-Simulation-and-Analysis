from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, struct

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel('FATAL')

df = spark.read.csv("data/test.csv",header=True,sep=",")
df = df.fillna( { 'Value':0, 'Type':0} )

df.show()

df.withColumn("Combination", struct(col("ID"), col("Type"))).groupBy('Combination').agg(_sum('Value').alias('Sum of Value')).show()


df.withColumn('Combination', struct(col('ID'), col('Type'))).groupBy('Combination').agg(_mean('Value').alias('Mean of Value')).show()

"""
df.limit(2).show()

df_agg = df.groupby(['ID']).agg(first(col('Type')).alias('Type'),
                                 first(col('Value')).alias('Value'),
                                 collect_set('Type').alias('Type_Arr'))

df_agg.show()                                                                                                                                                                                                                                   df_agg = df_agg.where(array_contains(col('Type_Arr'),'A') & (size(col('Type_Arr'))==1)).drop('Type_Arr')
df_agg.show()
"""
