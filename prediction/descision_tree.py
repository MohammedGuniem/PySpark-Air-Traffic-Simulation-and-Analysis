from pyspark import SparkContext, SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, collect_set, array_contains, size, first, sum as _sum, mean as _mean, desc, asc, count, concat_ws

if __name__ == "__main__":
    sc = SparkContext(appName="decision_tree_classification")
    sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("hdfs://master:9000/dataset/*.csv").show()
data2 = rawData
                .drop("ActualElapsedTime") // Forbidden
                .drop("ArrTime") // Forbidden
                .drop("AirTime") // Forbidden
                .drop("TaxiIn") // Forbidden
                .drop("Diverted") // Forbidden
                .drop("CarrierDelay") // Forbidden
                .drop("WeatherDelay") // Forbidden
                .drop("NASDelay") // Forbidden
                .drop("SecurityDelay") // Forbidden
                .drop("LateAircraftDelay") // Forbidden
                .drop("DepDelay") // Casted to double in a new variable called DepDelayDouble
                .drop("TaxiOut") // Casted to double in a new variable called TaxiOutDouble
                .drop("UniqueCarrier") // Always the same value // Remove correlated variables
                .drop("CancellationCode") // Cancelled flights don't count
                .drop("DepTime") // Highly correlated to CRSDeptime
                .drop("CRSArrTime") // Highly correlated to CRSDeptime
                .drop("CRSElapsedTime") // Highly correlated to Distance
                .drop("Distance") // Remove uncorrelated variables to the arrDelay
                .drop("FlightNum") // Remove uncorrelated variables to the arrDelay
                .drop("CRSDepTime") // Remove uncorrelated variables to the arrDelay
                .drop("Year") // Remove uncorrelated variables to the arrDelay
                .drop("Month") // Remove uncorrelated variables to the arrDelay
                .drop("DayofMonth") // Remove uncorrelated variables to the arrDelay
                .drop("DayOfWeek") // Remove uncorrelated variables to the arrDelay
                .drop("TailNum")
