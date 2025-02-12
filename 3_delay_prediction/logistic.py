
from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, OneHotEncoderEstimator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws, udf
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import sys,numpy as np
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import Evaluator
import pandas

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)
print("Logistic Regression classifier")
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', nullValue=' ').load("hdfs://master:9000/dataset/*.csv")
to_keep = ["QUARTER","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","TAXI_OUT", "DEP_DELAY","ARR_DELAY","OP_UNIQUE_CARRIER","OP_CARRIER_AIRLINE_ID","TAIL_NUM","OP_CARRIER_FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_ABR","ORIGIN_STATE_NM","DEST_AIRPORT_ID","DEST_AIRPORT_SEQ_ID","DEST","DEST_CITY_NAME","DEST_STATE_ABR","DEST_STATE_NM","CRS_DEP_TIME","CRS_ARR_TIME","CRS_ELAPSED_TIME","DISTANCE","DISTANCE_GROUP","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"]                                 
df = df.select(to_keep)
df = df.dropna()
df = df.withColumn('label', (df.ARR_DELAY > 15).cast('integer'))


org_indexer = StringIndexer(inputCol='ORIGIN_CITY_NAME', outputCol='org_idx').setHandleInvalid("keep")
des_indexer = StringIndexer(inputCol='DEST_CITY_NAME', outputCol='des_idx').setHandleInvalid("keep")
op_indexer = StringIndexer(inputCol='OP_UNIQUE_CARRIER', outputCol='op_idx').setHandleInvalid("keep")
tail_indexer = StringIndexer(inputCol='TAIL_NUM', outputCol='tail_idx').setHandleInvalid("keep")
origin_indexer = StringIndexer(inputCol='ORIGIN', outputCol='origin_idx').setHandleInvalid("keep")
orgABR_indexer = StringIndexer(inputCol='ORIGIN_STATE_ABR', outputCol='orgABR_idx').setHandleInvalid("keep")
orgNM_indexer = StringIndexer(inputCol='ORIGIN_STATE_NM', outputCol='orgNM_idx').setHandleInvalid("keep")
dest_indexer = StringIndexer(inputCol='DEST', outputCol='dest_idx').setHandleInvalid("keep")
destABR_indexer = StringIndexer(inputCol='DEST_STATE_ABR', outputCol='destABR_idx').setHandleInvalid("keep")
destNM_indexer = StringIndexer(inputCol='DEST_STATE_NM', outputCol='destNM_idx').setHandleInvalid("keep")



onehot = OneHotEncoderEstimator(inputCols=[org_indexer.getOutputCol(),
                                           des_indexer.getOutputCol(),
                                           op_indexer.getOutputCol(),
                                           tail_indexer.getOutputCol(),
                                           origin_indexer.getOutputCol(),
                                           orgABR_indexer.getOutputCol(),
                                           orgNM_indexer.getOutputCol(),
                                           dest_indexer.getOutputCol(),
                                           destABR_indexer.getOutputCol(),
                                           destNM_indexer.getOutputCol()],
                                outputCols=['org_dummy', 'des_dummy', 'op_dummy', 'tail_dummy', 'origin_dummy', 'orgABR_dummy', 'orgNM_dummy', 'dest_dummy', 'destABR_dummy'                                          , 'destNM_dummy'])

assembler = VectorAssembler(inputCols=['QUARTER','MONTH','ARR_DELAY','DEP_DELAY','DAY_OF_MONTH', 'DAY_OF_WEEK', 'TAXI_OUT', 'DISTANCE', 'OP_CARRIER_AIRLINE_ID', 'OP_CARRIER_FL_NUM', 'ORIGIN_AIRPORT_ID', 'ORIGIN_AIRPORT_SEQ_ID', 'DEST_AIRPORT_ID', 'DEST_AIRPORT_SEQ_ID', 'CRS_DEP_TIME', 'CRS_ARR_TIME', 'CARRIER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'WEATHER_DELAY', 'LATE_AIRCRAFT_DELAY', 'org_dummy', 'des_dummy', 'op_dummy', 'tail_dummy', 'origin_dummy', 'orgABR_dummy', 'orgNM_dummy', 'dest_dummy', 'destABR_dummy', 'destNM_dummy'],                                                                                                                                                                                    outputCol='features')  
lr = LogisticRegression(labelCol="label",featuresCol="features",maxIter=10,regParam=0.3)
pipeline = Pipeline(stages = [org_indexer, des_indexer, op_indexer, tail_indexer, origin_indexer, orgABR_indexer, orgNM_indexer, dest_indexer, destABR_indexer, destNM_indexer, onehot, assembler, lr])

train, test = df.randomSplit([0.7, 0.3], seed = 2019)
piplineModel = pipeline.fit(train)

prediction = piplineModel.transform(test)
predicted = prediction.select("features", "prediction", "label")
predicted.show(5, truncate=False)
TN = prediction.filter('prediction = 0 AND label = prediction').count()
TP = prediction.filter('prediction = 1 AND label = prediction').count()
FN = prediction.filter('prediction = 0 AND label <> prediction').count()
FP = prediction.filter('prediction = 1 AND label <> prediction').count()
print(' True Negative: %0.3f' % TN)
print(' True Positive: %0.3f' % TP)
print(' False Negative: %0.3f' % FN)
print(' False Positive: %0.3f' % FP)

accuracy = (TN + TP) / (TN + TP + FN + FP)
precision = TP / (TP + FP)
recall = TP / (TP + FN)
F =  2 * (precision*recall) / (precision + recall)
print(' precision: %0.3f' % precision)
print(' recall: %0.3f' % recall)
print(' accuracy: %0.3f' % accuracy)
print(' F1 score: %0.3f' % F)

evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
aur = evaluator.evaluate(prediction)
print ("AUR = ", aur)



paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.3, 0.1]).addGrid(lr.maxIter, [10, 5]).addGrid(lr.threshold, 
                                                                                            [0.4, 0.3]).build()
cv = CrossValidator(estimator=pipeline, evaluator=BinaryClassificationEvaluator(), estimatorParamMaps=paramGrid, 
                    numFolds=2)

model = cv.fit(train)

newprediction = model.transform(test)
newPredicted = prediction.select("features", "prediction", "label")
newPredicted.show(50)


TN = newprediction.filter('prediction = 0 AND label = prediction').count()
TP = newprediction.filter('prediction = 1 AND label = prediction').count()
FN = newprediction.filter('prediction = 0 AND label <> prediction').count()
FP = newprediction.filter('prediction = 1 AND label <> prediction').count()
print(' True Negative: %0.3f' % TN)
print(' True Positive: %0.3f' % TP)
print(' False Negative: %0.3f' % FN)
print(' False Positive: %0.3f' % FP)

accuracy = (TN + TP) / (TN + TP + FN + FP)
precision = TP / (TP + FP)
recall = TP / (TP + FN)
F =  2 * (precision*recall) / (precision + recall)
print(' precision: %0.3f' % precision)
print(' recall: %0.3f' % recall)
print(' accuracy: %0.3f' % accuracy)
print(' F1 score: %0.3f' % F)

evaluator2 = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
aur = evaluator2.evaluate(prediction)
print ("AUR2 = ", aur)

















