from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from datetime import datetime
# Import the ML algorithms we will use.
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils

sc = SparkContext()
sc.setLogLevel('FATAL')
sqlContext = SQLContext(sc)

now = datetime.now()
print("Started at: ", now)

"""
print("Reading data...")
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/2019_01.csv")
print("Count of rows: ", df.count())
"""

# Load and parse the data file, converting it to a DataFrame.
data = MLUtils.loadLibSVMFile(sc, "hdfs://master:9000/dataset/2019_01.csv").toDF()


# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="CARRIER_DELAY", outputCol="indexedLabel").fit(df)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="WHEELS_OFF", outputCol="indexedFeatures", maxCategories=4).fit(df)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df.randomSplit([0.7, 0.3])


#df2.show(20, False)
