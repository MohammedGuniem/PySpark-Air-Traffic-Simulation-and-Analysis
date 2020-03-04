import json
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws
from pyspark.sql import SQLContext

config_file = open("analyze_config.json")
config = json.load(config_file)
data_source = config["data_source"]

sc = SparkContext()

sc.setLogLevel('FATAL')

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(data_source)

df = df.fillna( { 'CARRIER_DELAY':0,'WEATHER_DELAY':0,'NAS_DELAY':0,'SECURITY_DELAY':0,'LATE_AIRCRAFT_DELAY':0 } )

showRowCount = 10

for target in config["analyze_targets"]:
    description = target["description"]
    target_column = target["target_column"]
    target_type = target["target_type"]
    target_name = target["target_name"]
    hdfs_csv_writing_paths = target["csv_writing_paths"]["hdfs"]
    local_csv_writing_paths = target["csv_writing_paths"]["local"]

    print("Started: " + description)

    delay_df = df.withColumn("TOTAL_DELAY", col("WEATHER_DELAY")+col("CARRIER_DELAY")+col('NAS_DELAY')+col('SECURITY_DELAY')+col('LATE_AIRCRAFT_DELAY'))

    if target_type == "merged":
        target_components = target["target_components"]
        target_separator = target["target_separator"]

        delay_df = delay_df.withColumn(target_column, concat_ws(target_separator, target_components[0], target_components[1]))
        for index in range(2,len(target_components)):
            delay_df = delay_df.withColumn(target_column, concat_ws(target_separator, target_column, target_components[index]))

    delay_df = delay_df.groupBy(target_column)
    
    delay_df = delay_df.agg(
		_sum('TOTAL_DELAY').alias('SUM_OF_ALL_DELAYS'),_mean('TOTAL_DELAY').alias('AVERAGE_OF_ALL_DELAYS'),
		_sum('WEATHER_DELAY').alias("SUM_OF_WEATHER_DELAY"),_mean('WEATHER_DELAY').alias('AVERAGE_OF_WEATHER_DELAY'),
		_sum('CARRIER_DELAY').alias("SUM_OF_CARRIER_DELAY"),_mean('CARRIER_DELAY').alias('AVERAGE_OF_CARRIER_DELAY'),
		_sum('NAS_DELAY').alias("SUM_OF_NAS_DELAY"),_mean('NAS_DELAY').alias('AVERAGE_OF_NAS_DELAY'),
		_sum('SECURITY_DELAY').alias("SUM_OF_SECURITY_DELAY"),_mean('SECURITY_DELAY').alias('AVERAGE_OF_SECURITY_DELAY'),
		_sum('LATE_AIRCRAFT_DELAY').alias("SUM_OF_LATE_AIRCRAFT_DELAY"),_mean('LATE_AIRCRAFT_DELAY').alias('AVERAGE_OF_LATE_AIRCRAFT_DELAY')
	).sort(desc("SUM_OF_ALL_DELAYS"))
    
    delay_df.show(showRowCount, False)
    
    print("The total number of " + target_name + " in dataset is: " + str(delay_df.count()))

    print("Writing csv file on hdfs ...")
    delay_df.repartition(1).write.csv(hdfs_csv_writing_paths)

    print("Writing csv file on local machine ...")
    delay_df.repartition(1).write.csv(local_csv_writing_paths, header = 'true')

    print("Done: " + description)

