import pyspark
from pyspark import SparkConf,      \
                    SparkContext,   \
                    SparkFiles
from pyspark.sql import SQLContext, Row
from pandas import read_csv

# App Environment
MASTER_URL  = "spark://front-in1.cemef:7077"
APP_NAME    = "IRIS-ML"
DATA_DIR    = "/gext/rami.kader/hpcai/HPDA/\
                BigDataHadoopSparkDaskCourse/\
                TPs/Project/iris.csv"

# Spark Context & Conf
conf        = SparkConf().setMaster(MASTER_URL).setAppName(APP_NAME)
sc          = SparkContext(conf=conf)
sqlContext  = SQLContext(sc)

# Data Setup
iris_df     = sqlContext.createDataFrame(read_csv(DATA_DIR))
iris_df.printSchema()

# Add a numeric indexer for the label/target column
from pyspark.ml.feature import StringIndexer
stringIndexer   = StringIndexer(inputCol="variety", outputCol="ind_variety")
si_model        = stringIndexer.fit(iris_df)
irisNormDf      = si_model.transform(iris_df)
irisNormDf.printSchema()
irisNormDf.select("variety", "ind_variety").distinct().collect()

# Perform Data Analytics
irisNormDf.describe().show()

# Transform to a Data Frame for input to Machine Learing
# Drop columns that are not required (low correlation)
from pyspark.ml.linalg import Vectors
def transformToLabeledPoint(row) :
    lp = ( row["variety"], row["ind_variety"], \
                Vectors.dense([row["sepal_length"],\
                        row["sepal_width"], \
                        row["petal_length"], \
                        row["petal_width"]]))
    return lp

irisLp = irisNormDf.rdd.map(transformToLabeledPoint)
irisLpDf = sqlContext.createDataFrame(irisLp,["species","label", "features"])
irisLpDf.select("species","label","features").show(50)
irisLpDf.cache()

# Split into training and testing data
(trainingData, testData) = irisLpDf.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()
testData.collect()

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=4, labelCol="label",\
                featuresCol="features")
dtModel = dtClassifer.fit(trainingData)

print(dtModel.numNodes)
print(dtModel.depth)

# Predict on the test data
predictions = dtModel.transform(testData)
predictions.select("prediction","species","label").collect()

# Evaluate accuracy
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
evaluator.evaluate(predictions)    

# Draw a confusion matrix
predictions.groupBy("label","prediction").count().show()

# Closing Spark Context
sc.stop()