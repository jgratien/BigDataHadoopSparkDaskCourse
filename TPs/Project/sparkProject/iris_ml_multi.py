# Import required libraries :
import pyspark
import pandas as pd 
import os
import sys

from pyspark.ml import Pipeline
from pyspark import SparkContext, SparkFiles
from pyspark.sql import Row, SQLContext
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#====================================================================================

# Create spark context : ============================================================
sc = SparkContext()
sqlContext = SQLContext(sc)

# Import data :
iris_df = sqlContext.read.csv(SparkFiles.get('/user/abdessalam/project/iris.csv'),inferSchema=True, header=True)
iris_df.na.drop()

str_indexer = StringIndexer(inputCol='variety', outputCol='ind_variety')
strInd_model = str_indexer.fit(iris_df)
iris_df_ind = strInd_model.transform(iris_df)
iris_df.cache()

# Data Exploration : ================================================================

## Data schema
print("::::: Data schema :::::\n") 
iris_df_ind.printSchema()
iris_df_ind.show(10)

## Data description
print("::::: Data decription :::::\n") 
iris_df_ind.describe().show()
iris_df_ind.cache()

## Data correlation :
iris_rdd = iris_df_ind.rdd.map(lambda row : (row["variety"], \
    Vectors.dense([row["ind_variety"], \
                    row["sepal_length"], \
                    row["sepal_width"], \
                    row["petal_length"], \
                    row["petal_width"]])))

iris_ft = sqlContext.createDataFrame(iris_rdd, ["type", "target&features"])

corr_matrix = Correlation.corr(iris_ft, "target&features").head()
print("::::: Correlation Matrix :::::\n", str(corr_matrix[0]))
iris_ft.cache()

# Data Preparation pipeline : ======================================================

## String Indexer :
str_indexer = StringIndexer(inputCol='variety', outputCol='label')

## vector Assembling :
assembler = VectorAssembler(inputCols=["sepal_length","sepal_width","petal_length","petal_width"], outputCol='features')

## Feature scaling :
scaler = StandardScaler(inputCol=assembler.getOutputCol(), outputCol='scaled_features', withStd=True)

## Setting up diverse models :
lr = LogisticRegression(labelCol="label",featuresCol="scaled_features",maxIter=10, regParam=0.3)
dt = DecisionTreeClassifier(labelCol="label", featuresCol="scaled_features")
rf = RandomForestClassifier(labelCol="label", featuresCol="scaled_features", maxDepth=4)
"""gbt = GBTClassifier(labelCol="label", featuresCol="scaled_features", maxIter=10) #Not supported yet for multiclassification"""
models = {'lr': lr, 'dt': dt, 'rf': rf}

## Creating the pipeline for the chosen model :
model = sys.argv[1]
if (model in models.keys()):
    None
else:
    raise ValueError("The model chosen is not taken into consideration")

stages = [str_indexer, assembler, scaler, models[model]]
pipeline = Pipeline(stages=stages)

# Classification models benchmark : ================================================

## Test, val data splitting :
(train_data, test_data) = iris_df.randomSplit([0.8, 0.2])

## Model training :
ml_model = pipeline.fit(train_data)

## Model prediction :
predictions = ml_model.transform(test_data)

## Model evaluation :
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy")
evaluation = evaluator.evaluate(predictions)
    
print("Model : {0} .\n :::::: Accuracy = {1}".format(model, evaluation))
