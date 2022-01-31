import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkFiles
import os
import pandas as pd

"""----------------------------------------------------------------------------
CREATE SPARK CONTEXT
CREATE SQL CONTEXT
----------------------------------------------------------------------------"""
sc =SparkContext()
sqlContext = SQLContext(sc)

"""----------------------------------------------------------------------------
LOAD IRIS DATA
----------------------------------------------------------------------------"""
data_dir="/home/aympab/repos/BigDataHadoopSparkDaskCourse/TPs/Project"
file = os.path.join(data_dir,"iris.csv")
panda_df = pd.read_csv(file)

iris_df=sqlContext.createDataFrame(panda_df)
iris_df.printSchema()

#Add a numeric indexer for the label/target column
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="variety", outputCol="ind_variety")
si_model = stringIndexer.fit(iris_df)
irisNormDf = si_model.transform(iris_df)
irisNormDf.printSchema()
irisNormDf.select("variety","ind_variety").distinct().collect()
#irisNormDf.cache()

"""--------------------------------------------------------------------------
Perform Data Analytics
-------------------------------------------------------------------------"""

#See standard parameters
#irisNormDf.describe().show()


"""--------------------------------------------------------------------------
Prepare data for ML
-------------------------------------------------------------------------"""

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

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

"""--------------------------------------------------------------------------
Perform Machine Learning
-------------------------------------------------------------------------"""

#Split into training and testing data
(trainingData, testData) = irisLpDf.randomSplit([0.7, 0.3])
trainingData.count()
testData.count()
testData.collect()

from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=4, labelCol="label",\
                featuresCol="features")
dtModel = dtClassifer.fit(trainingData)

rfClasifier = RandomForestClassifier(maxDepth=4, labelCol="label",\
                featuresCol="features")
rfModel = rfClasifier.fit(trainingData)

layers = [4, 5, 4, 3]
mlp = MultilayerPerceptronClassifier(maxIter=100, layers=layers, labelCol="label", featuresCol="features")
mlpModel = mlp.fit(trainingData)
# gbtClassifier = GBTClassifier(maxDepth=4, labelCol="label",\
#                 featuresCol="features")
# gbtModel = gbtClassifier.fit(trainingData)

# print(rfModel.numNodes)
# print(rfModel.depth)

print(dtModel.numNodes)
print(dtModel.depth)

#Predict on the test data
predictions = dtModel.transform(testData)
predictions.select("prediction","species","label").collect()

predictions_rf = rfModel.transform(testData)
predictions_rf.select("prediction","species","label").collect()

predictions_mlp = mlpModel.transform(testData)
predictions_mlp.select("prediction","species","label").collect()

#Evaluate accuracy and Draw a confusion matrix
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
print(" >>>  DECISION TREES :\nscore : {:.3f}".format(evaluator.evaluate(predictions)))
predictions.groupBy("label","prediction").count().show()

evaluator_rf = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
print(" >>> RANDOM FOREST :\nscore : {:.3f}".format(evaluator_rf.evaluate(predictions_rf)))
predictions_rf.groupBy("label","prediction").count().show()

evaluator_mlp = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
print(" >>> MLP :\nscore : {:.3f}".format(evaluator_mlp.evaluate(predictions_mlp)))
predictions_mlp.groupBy("label","prediction").count().show()

sc.stop()