from pyspark import SparkContext, SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import sys,numpy as np


if __name__ == "__main__":
    sc = SparkContext(appName="decision_tree_classification")
    sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/*.csv")
data2 = df.drop("ACTUAL_ELAPSED_TIME").drop("YEAR").drop("QUARTER").drop("DEST").drop("AIRTIME").drop("OP_CARRIER_AIRLINE_ID").drop("TAXI_IN").drop("DIVERTED").drop("CARRIER_DELAY").drop("WEATHER_DELAY").drop("NAS_DELAY").drop("SECURITY_DELAY").drop("LATE_AIRCRAFT_DELAY").drop("OP_UNIQUE_CARRIER").drop("CANCELLATION_CODE").drop("WHEELS_ON").drop("WHEELS_OFF").drop("CRS_ELAPSED_TIME").drop("DISTANCE_GROUP").drop("_c45").drop("CRS_DEP_TIM").drop("MONTH").drop("DAY_OF_MONTH").drop("DAY_OF_WEEK").drop("TAIL_NUM").drop("FL_DATE").drop("OP_CARRIER_FL_NUM").drop("ORIGIN_AIRPORT_ID").drop("ORIGIN_AIRPORT_SEQ_ID").drop("ORIGIN").drop("ORIGIN_CITY_NAME").drop("ORIGIN_STATE_ABR").drop("ORIGIN_STATE_NM").drop("DEST_AIRPORT_ID").drop("DEST_AIRPORT_SEQ_ID").drop("DEST_CITY_NAME").drop("DEST_STATE_ABR").drop("DEST_STATE_NM").drop("CRS_DEP_TIME").drop("DEP_TIME").drop("CRS_ARR_TIME").drop("ARR_TIME").drop("CANCELLED").drop("AIR_TIME").drop("DISTANCE")

delay = data2.show()

assembler = VectorAssembler(                                               
    inputCols=["DEP_DELAY", "TAXI_OUT"],
    outputCol="ARR_DELAY")

output = assembler.transform(delay)
output.select("DEP_DELAY", "ARR_DELAY").show(truncate=False)





