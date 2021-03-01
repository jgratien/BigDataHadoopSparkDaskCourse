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
data_dir="/gext/jean-marc.gratien/BigDataHadoopSpark/TPs/data"
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
(trainingData, testData) = irisLpDf.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()
testData.collect()

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=4, labelCol="label",\
                featuresCol="features")
dtModel = dtClassifer.fit(trainingData)

print(dtModel.numNodes)
print(dtModel.depth)

#Predict on the test data
predictions = dtModel.transform(testData)
predictions.select("prediction","species","label").collect()

#Evaluate accuracy
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
evaluator.evaluate(predictions)    

#Draw a confusion matrix
predictions.groupBy("label","prediction").count().show()

