import json
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, array_contains, size, first, sum as _sum, mean as _mean, max as _max, min as _min, desc, asc, count, concat_ws
from pyspark.sql import SQLContext
from datetime import datetime

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
    
    now = datetime.now()
    print("Started: ", description, " at ", now)

    delay_df = df.withColumn("TOTAL_DELAY", col("WEATHER_DELAY")+col("CARRIER_DELAY")+col('NAS_DELAY')+col('SECURITY_DELAY')+col('LATE_AIRCRAFT_DELAY'))

    if target_type == "merged":
        target_components = target["target_components"]
        target_separator = target["target_separator"]

        delay_df = delay_df.withColumn(target_column, concat_ws(target_separator, target_components[0], target_components[1]))
        for index in range(2,len(target_components)):
            delay_df = delay_df.withColumn(target_column, concat_ws(target_separator, target_column, target_components[index]))

    delay_df = delay_df.groupBy(target_column)
    
    delay_df = delay_df.agg(
		_sum('TOTAL_DELAY').alias('SUM OF ALL DELAYS'),_mean('TOTAL_DELAY').alias('AVERAGE OF ALL DELAYS'),_min('TOTAL_DELAY').alias("MIN OF ALL DELAYS"),_max('TOTAL_DELAY').alias("MAX OF ALL DELAYS"),
       	        _sum('WEATHER_DELAY').alias("SUM OF WEATHER DELAY"),_mean('WEATHER_DELAY').alias('AVERAGE OF WEATHER DELAY'),_min('WEATHER_DELAY').alias("MIN OF WEATHER DELAY"),_max('WEATHER_DELAY').alias("MAX OF WEATHER DELAY"),	
                _sum('CARRIER_DELAY').alias("SUM OF CARRIER DELAY"),_mean('CARRIER_DELAY').alias('AVERAGE OF CARRIER DELAY'),_min('CARRIER_DELAY').alias("MIN OF CARRIER DELAY"),_max('CARRIER_DELAY').alias("MAX OF CARRIER DELAY"),
                _sum('NAS_DELAY').alias("SUM OF NAS DELAY"),_mean('NAS_DELAY').alias('AVERAGE OF NAS DELAY'),_min('NAS_DELAY').alias("MIN OF NAS DELAY"),_max('NAS_DELAY').alias("MAX OF NAS DELAY"),
                _sum('SECURITY_DELAY').alias("SUM OF SECURITY DELAY"),_mean('SECURITY_DELAY').alias('AVERAGE OF SECURITY DELAY'),_min('SECURITY_DELAY').alias("MIN OF SECURITY DELAY"),_max('SECURITY_DELAY').alias("MAX OF SECURITY DELAY"),
                _sum('LATE_AIRCRAFT_DELAY').alias("SUM OF LATE AIRCRAFT DELAY"),_mean('LATE_AIRCRAFT_DELAY').alias('AVERAGE OF LATE AIRCRAFT DELAY'),
                _min('LATE_AIRCRAFT_DELAY').alias("MIN OF LATE AIRCRAFT DELAY"),_max('LATE_AIRCRAFT_DELAY').alias("MAX OF LATE AIRCRAFT DELAY")
	).sort(desc("SUM OF ALL DELAYS"))
    
    delay_df = delay_df.withColumnRenamed(target_column, target_column.replace("_", " "))

#    delay_df.show(showRowCount, False)
    
    print("The total number of " + target_name + " in dataset is: " + str(delay_df.count()))

    print("Writing csv file on hdfs ...")
    delay_df.repartition(1).write.csv(hdfs_csv_writing_paths)

    print("Writing csv file on local machine ...")
    delay_df.repartition(1).write.csv(local_csv_writing_paths, header = 'true')
  
    now = datetime.now()
    print("Done: ", description, " at ", now)

