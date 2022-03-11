from pyspark import SparkContext, SparkFiles
from pyspark.sql import Row, SQLContext
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
    OneVsRest,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pyspark
import pandas as pd
from pathlib import Path

PATH = Path().absolute() / "data"


def transformToLabeledPoint(row):
    """
    Drop columns that are not required (low correlation)
    """
    attribs = ("sepal_length", "sepal_width", "petal_length", "petal_width")
    elements = [row[attrib] for attrib in attribs]
    return (row["variety"], row["ind_variety"], Vectors.dense(elements))


# CREATE SPARK CONTEXT
# CREATE SQL CONTEXT
sc = SparkContext()
sqlContext = SQLContext(sc)

# LOAD IRIS DATA
iris_df = sqlContext.createDataFrame(pd.read_csv(PATH / "iris.csv"))
iris_df.printSchema()

# Add a numeric indexer for the label/target column
stringIndexer = StringIndexer(inputCol="variety", outputCol="ind_variety")
irisNormDf = stringIndexer.fit(iris_df).transform(iris_df)
irisNormDf.printSchema()
irisNormDf.select("variety", "ind_variety").distinct().collect()
# irisNormDf.cache()

# Perform Data Analytics

# See standard parameters
irisNormDf.describe().show()

# Prepare data for ML and transform to a Data Frame for input to ML
irisLp = irisNormDf.rdd.map(transformToLabeledPoint)
irisLpDf = sqlContext.createDataFrame(irisLp, ["species", "label", "features"])
irisLpDf.select("species", "label", "features").show(50)
# irisLpDf.cache()

# Perform Machine Learning

# Split into training and testing data
trainingData, testData = irisLpDf.randomSplit([0.8, 0.2])
trainingData.count()
testData.count()
testData.collect()

# Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=4, labelCol="label", featuresCol="features")
rfClassifer = RandomForestClassifier(maxDepth=4, labelCol="label", featuresCol="features")
gbtClassifer = GBTClassifier(maxDepth=4, labelCol="label", featuresCol="features")
ovr = OneVsRest(classifier=gbtClassifer, labelCol="label", featuresCol="features")
dtModel = dtClassifer.fit(trainingData)
rfModel = rfClassifer.fit(trainingData)
# gbtModel = gbtClassifer.fit(trainingData)

# ovr.getRawPredictionCol()
ovr.setPredictionCol("prediction")
gbtModel = ovr.fit(trainingData)

print("Numbers of Nodes : {}".format(dtModel.numNodes))
print("Depth : {}".format(dtModel.depth))

# Predict on the test data
predictions = dtModel.transform(testData)
predictions_rf = rfModel.transform(testData)
predictions_gbt = gbtModel.transform(testData)
predictions.select("prediction", "species", "label").collect()
predictions_rf.select("prediction", "species", "label").collect()
predictions_gbt.select("prediction", "species", "label").collect()

# Evaluate accuracy
params = {"predictionCol": "prediction", "labelCol": "label", "metricName": "accuracy"}
evaluator = MulticlassClassificationEvaluator(**params)
evaluator.evaluate(predictions)

# Draw a confusion matrix
predictions.groupBy("label", "prediction").count().show()

# Evaluate accuracy
evaluator.evaluate(predictions_rf)

# Draw a confusion matrix
predictions_rf.groupBy("label", "prediction").count().show()


# Evaluate accuracy
evaluator.evaluate(predictions_gbt)

# Draw a confusion matrix
predictions_gbt.groupBy("label", "prediction").count().show()
