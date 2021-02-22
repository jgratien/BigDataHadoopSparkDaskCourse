from pandas import read_csv
from pyspark import SparkConf,      \
                    SparkContext,   \
                    SparkFiles
from pyspark.sql import SQLContext, Row
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier,   \
                                      RandomForestClassifier,   \
                                      GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# App Environment
MASTER_URL  = "spark://front-in1.cemef:7077"
APP_NAME    = "IRIS-ML"
DATA_DIR    = "/gext/rami.kader/hpcai/HPDA/BigDataHadoopSparkDaskCourse/TPs/Project/iris.csv"

# Spark Context & Conf
print("Setting Spark Context...")
conf        = SparkConf().setMaster(MASTER_URL).setAppName(APP_NAME)
sc          = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext  = SQLContext(sc)

# Data Load & Setup
print("Loading data...")
iris_df     = sqlContext.createDataFrame(read_csv(DATA_DIR))

# Transform to a Data Frame for input to Machine Learing
iris_lp = sqlContext.createDataFrame(
    iris_df.rdd.map(
        lambda row : (
            row["variety"],
            Vectors.dense([
                row["sepal_length"],
                row["sepal_width"],
                row["petal_length"],
                row["petal_width"]]))
    ),
    ["species", "features"])

# Add a numeric indexer for the label/target column
labelIndexer = StringIndexer(inputCol="species", outputCol="label")

# Split into training and testing data
(trainingData, testData) = iris_lp.randomSplit([0.75, 0.25])

# Create the models
dtClass     = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=4)
rfClass     = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20)
gbtClass    = GBTClassifier(labelCol="label", featuresCol="features", maxIter=15)

# ML-Pipelines
print("Creating training pipelines...")
dtPipe  = Pipeline(stages=[labelIndexer, dtClass])
rfPipe  = Pipeline(stages=[labelIndexer, rfClass])
gbtPipe = Pipeline(stages=[labelIndexer, gbtClass])

# Models objects output
print("Fitting pipelines...")
dtModel     = dtPipe.fit(trainingData)
rfModel     = rfPipe.fit(trainingData)
gbtModel    = gbtPipe.fit(trainingData)

# Predict on the test data
print("Performing predictions...")
dt_predictions  = dtModel.transform(testData)
rf_predictions  = rfModel.transform(testData)
gbt_predictions = gbtModel.transform(testData)

# Evaluate accuracy
evaluator = MulticlassClassificationEvaluator(
    predictionCol="prediction",
    labelCol="label",
    metricName="accuracy")
print("--- Classifiers Test Errors ---")
print("Decision Tree: %g " % (1.0 - evaluator.evaluate(dt_predictions)))
print("Random Forest: %g " % (1.0 - evaluator.evaluate(rf_predictions)))
print("GBoosted Tree: %g " % (1.0 - evaluator.evaluate(gbt_predictions)))

# Closing Spark Context
sc.stop()