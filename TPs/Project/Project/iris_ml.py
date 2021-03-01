from pandas import read_csv
from pyspark import SparkConf,      \
                    SparkContext,   \
                    SparkFiles
from pyspark.sql import SQLContext, Row
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer,   \
                               VectorAssembler, \
                               StandardScaler
from pyspark.ml.classification import DecisionTreeClassifier,   \
                                      LogisticRegression,   \
                                      RandomForestClassifier 
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import pandas as pd

sc = SparkContext()
sqlContext = SQLContext(sc)

pandas_data = pd.read_csv("iris.csv")
data = sqlContext.createDataFrame(pandas_data)
print("Original Data's Schema /n")
data.printSchema()

train, test = data.randomSplit([0.8, 0.2])

stringIndexer = StringIndexer(inputCol="variety", outputCol="num_variety")
indexed = stringIndexer.fit(data).transform(data)


vectorAssembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],\
                                  outputCol="features")
assembled = vectorAssembler.transform(indexed)
assembled = assembled.drop('sepal_length', 'sepal_width', 'petal_length', 'petal_width')

##Displaying the effect of preprocessing to understand how spark deals with data

print("Data's Schema after PreProcessing Phase")
assembled.show()

DT_clf = DecisionTreeClassifier(labelCol="num_variety", featuresCol="features", maxDepth=10, maxBins=32)

LR_clf = LogisticRegression( labelCol="num_variety", featuresCol="features",maxIter=20, regParam=0.8)

RF_clf = RandomForestClassifier(labelCol="num_variety", featuresCol="features", maxDepth=6, numTrees=50)

models = [DT_clf, LR_clf, RF_clf]

evaluator = MulticlassClassificationEvaluator(labelCol="num_variety", predictionCol="prediction", metricName="accuracy")

names = ["Decision Trees", "Logistic Regression", "Random Forests"]
i=0
for mod in models:

  pipe = Pipeline(stages=[stringIndexer, vectorAssembler, mod])
  model = pipe.fit(train)

  # obtain predictions.
  pred = model.transform(test)

  # Columns to display for evaluation
  pred.select("prediction", "num_variety", "features").show(20)
  
  # Accuracy for comparison between models
  accuracy = evaluator.evaluate(pred)

  print(names[i])
  i+=1
  print("Test accuracy = ", accuracy*100, "%\n")

