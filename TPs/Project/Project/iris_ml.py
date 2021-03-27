# Importing relevant libraries
import pandas as pd

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import DecisionTreeClassifier,  LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

###################### SPARK CONFIGS #####################

# Spark Local Configuration
sconf = SparkConf()
sconf.setAppName("irisPreds")
sconf.setMaster("local[1]")
sconf.set('spark.executor.memory', '4g')
sconf.set('spark.executor.cores', 8)
sconf.set('spark.logConf', True)

# Spark Context
sc = SparkContext(conf=sconf)
sqlContext = SQLContext(sc)

###################### DATA PIPELINES #####################

# Data Load & Setup
print("Loading data Started")
irisData = sqlContext.createDataFrame(pd.read_csv("iris.csv"))

print("Creating preprocessing pipeline")

# Gathering features together into ML models supported format (inputCols)
vecAssembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
                               outputCol="features")

# Numeric indexing of the target variable
labellizer = StringIndexer(inputCol="variety",
                           outputCol="label")
# Scaling features set (mean + std)
scaler = StandardScaler(inputCol="features",
                        outputCol="scaled",
                        withStd=True,
                        withMean=True)

# Splitting into training/testing sets
(train, test) = irisData.randomSplit([0.7, 0.3])

# Aggregated Preprocessing Pipeline
dataPreprocessor = Pipeline(
    stages=[vecAssembler, labellizer, scaler]).fit(train)

print("Preprocessing pipeline Created")

###################### ML PIPELINES #####################

# ML Models
# 1 - Decision Tree
dtClassifier = DecisionTreeClassifier(labelCol="label",
                                      featuresCol="scaled",
                                      maxDepth=4)
# 2 - Logistic Regression
lrClassifier = LogisticRegression(labelCol="label",
                                  featuresCol="scaled",
                                  maxIter=30)

# 3 - Random Forest
rfClassifier = RandomForestClassifier(labelCol="label",
                                      featuresCol="scaled",
                                      numTrees=20,
                                      maxDepth=3,
                                      minInstancesPerNode=6)

# Data Preprocessing Pipeline
readyTrain = dataPreprocessor.transform(train)
readyTest = dataPreprocessor.transform(test)

# Models objects fitting
dtModel = dtClassifier.fit(readyTrain)
lrModel = lrClassifier.fit(readyTrain)
rfModel = rfClassifier.fit(readyTrain)

# Predicting on the test data
dtPreds = dtModel.transform(readyTest)
lrPreds = lrModel.transform(readyTest)
rfPreds = rfModel.transform(readyTest)

# Predicting on the train data
dtPredsTrain = dtModel.transform(readyTrain)
lrPredsTrain = lrModel.transform(readyTrain)
rfPredsTrain = rfModel.transform(readyTrain)

# Evaluating accuracy
accEvaluator = MulticlassClassificationEvaluator(predictionCol="prediction",
                                                 labelCol="label",
                                                 metricName="accuracy")

###################### EVALUATION AND RESULTS #####################

dtTrainAcc = accEvaluator.evaluate(dtPredsTrain)
dtTestAcc = accEvaluator.evaluate(dtPreds)

lrTrainAcc = accEvaluator.evaluate(lrPredsTrain)
lrTestAcc = accEvaluator.evaluate(lrPreds)

rfTrainAcc = accEvaluator.evaluate(rfPredsTrain)
rfTestAcc = accEvaluator.evaluate(rfPreds)

print("[TRAIN] Decision Tree: ", dtTrainAcc)
print("[TRAIN] Logistic Regression: ", lrTrainAcc)
print("[TRAIN] Random Forest: ", rfTrainAcc)


print("[TEST] Decision Tree: ", dtTestAcc)
print("[TEST] Logistic Regression: ", lrTestAcc)
print("[TEST] Random Forest: ", rfTestAcc)

# Closing Spark Context
sc.stop()