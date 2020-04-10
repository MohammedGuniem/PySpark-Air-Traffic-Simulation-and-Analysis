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
from pyspark.mllib.evaluation import BinaryClassificationMetrics as metric


sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)
print("Starting decision Tree classifier")

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', nullValue=' ').load("hdfs://master:9000/dataset/*.csv")

to_keep = ["QUARTER","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","TAXI_OUT", "DEP_DELAY","ARR_DELAY","OP_UNIQUE_CARRIER","OP_CARRIER_AIRLINE_ID","TAIL_NUM","OP_CARRIER_FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_ABR","ORIGIN_STATE_NM","DEST_AIRPORT_ID","DEST_AIRPORT_SEQ_ID","DEST","DEST_CITY_NAME","DEST_STATE_ABR","DEST_STATE_NM","CRS_DEP_TIME","CRS_ARR_TIME","CRS_ELAPSED_TIME","DISTANCE","DISTANCE_GROUP","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"]
df = df.select(to_keep)
df = df.dropna()
df = df.withColumn('label', (df.ARR_DELAY > 15).cast('integer'))

(train, test) = df.randomSplit([0.8, 0.2], seed=12345)

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

rf = RandomForestClassifier(labelCol = "label", featuresCol = "features", seed = 54321, maxDepth = 3)

pipeline = Pipeline(stages = [org_indexer, des_indexer, op_indexer, tail_indexer, origin_indexer, orgABR_indexer, orgNM_indexer, dest_indexer, destABR_indexer, destNM_indexer, onehot, assembler, rf])                                                                                                                                                 
evaluatorPR = BinaryClassificationEvaluator(labelCol = "label", rawPredictionCol = "prediction", metricName = "areaUnderPR")
evaluatorAUC = BinaryClassificationEvaluator(labelCol = "label", rawPredictionCol = "prediction", metricName = "areaUnderROC")

paramGrid = ParamGridBuilder() \
.addGrid(rf.maxDepth, [5, 10, 15]) \
.addGrid(rf.maxBins, [10, 20, 30]) \
.build()

# Build out the cross validation
crossval = CrossValidator(estimator = rf,
                          estimatorParamMaps = paramGrid,
                          evaluator = evaluatorPR,
                          numFolds = 3)  
# Build the CV pipeline
pipelineCV =  Pipeline(stages = [org_indexer, des_indexer, op_indexer, tail_indexer, origin_indexer, orgABR_indexer, orgNM_indexer, dest_indexer, destABR_indexer, destNM_indexer, onehot, assembler, crossval])

# Train the model using the pipeline, parameter grid, and preceding BinaryClassificationEvaluator
cvModel_u = pipelineCV.fit(train)

# Build the best model (training and test datasets)
train_pred = cvModel_u.transform(train)
test_pred = cvModel_u.transform(test)

# Evaluate the model on training datasets
pr_train = evaluatorPR.evaluate(train_pred)
auc_train = evaluatorAUC.evaluate(train_pred)

# Evaluate the model on test datasets
pr_test = evaluatorPR.evaluate(test_pred)
auc_test = evaluatorAUC.evaluate(test_pred)

# Print out the PR and AUC values
print("PR train:", pr_train)
print("AUC train:", auc_train)
print("PR test:", pr_test)
print("AUC test:", auc_test)

