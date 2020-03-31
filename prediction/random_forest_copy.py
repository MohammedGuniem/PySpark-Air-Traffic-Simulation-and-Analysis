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

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)
print("Categorizing the delays from dataset")
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', nullValue=' ').load("hdfs://master:9000/dataset/*.csv")
to_keep = ['MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'TAXI_OUT', 'DISTANCE', 'DEST_CITY_NAME', 'ORIGIN_CITY_NAME', 'DEP_DELAY', 'ARR_DELAY']
df = df.select(to_keep)
df = df.dropna()
df = df.withColumn('label', (df.ARR_DELAY > 15).cast('integer'))
num_0 = df.filter(df.label==0).count()
ratio = (df.count() - num_0)/num_0
df = df.sampleBy('label', {0: ratio, 1:1})
df.show(10)

org_indexer = StringIndexer(inputCol='ORIGIN_CITY_NAME', outputCol='org_idx').setHandleInvalid("keep")
des_indexer = StringIndexer(inputCol='DEST_CITY_NAME', outputCol='des_idx').setHandleInvalid("keep")
onehot = OneHotEncoderEstimator(inputCols=[org_indexer.getOutputCol(),
                                           des_indexer.getOutputCol()],
                                outputCols=['org_dummy', 'des_dummy'])


assembler = VectorAssembler(inputCols=['MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
                                       'org_dummy', 'des_dummy',
                                       'TAXI_OUT',
                                       'DEP_DELAY', 'DISTANCE',
                                       'ARR_DELAY'],
                            outputCol='features')                                                       


pipeline = Pipeline(stages = [org_indexer, des_indexer,
                            onehot, assembler])
pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)
selectedCols = ['label', 'features'] + to_keep
df = df.select(selectedCols)
df.printSchema()                                                                                                                       

train, test = df.randomSplit([0.7, 0.3], seed = 2019)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
rfModel = rf.fit(train)
predictions = rfModel.transform(test)
predictions.select('TAXI_OUT', 'DEP_DELAY', 'label', 'rawPrediction', 'prediction', 'probability').show(10)   

evaluator = BinaryClassificationEvaluator()
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

<<<<<<< HEAD

paramGrid = ParamGridBuilder.build()

cv = CrossValidator(estimator=RandomForestClassifier, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
cvModel = cv.fit(train)
predictions = cvModel.transform(test)
evaluator.evaluate(predictions)
=======
>>>>>>> 8aeacc4b5ffa30247dfbb6022ee60a7743cda407
                                                                                                                                                                                                                                                                                                                                        
