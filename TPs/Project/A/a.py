from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
import time
import matplotlib.pyplot as plt

# Initialize SparkContext and SQLContext
sc = SparkContext(appName="IrisML")
sqlContext = SQLContext(sc)

# Load the data
iris_df = sqlContext.read.csv("iris.csv", header=True, inferSchema=True)

# Define preprocessing stages
indexer = StringIndexer(inputCol="variety", outputCol="label")
assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")

# Split the data into training and test sets
train_df, test_df = iris_df.randomSplit([0.7, 0.3], seed=42)

# Function to create and evaluate a pipeline
def evaluate_pipeline(classifier, classifier_name):
    # Create a pipeline with preprocessing stages and the classifier
    pipeline = Pipeline(stages=[indexer, assembler, classifier])
    
    # Record start time
    start_time = time.time()
    
    # Fit the pipeline on the training data
    model = pipeline.fit(train_df)
    
    # Make predictions on the test data
    predictions = model.transform(test_df)
    
    # Calculate execution time
    execution_time = time.time() - start_time
    
    # Evaluate accuracy
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    
    # Return results
    return classifier_name, accuracy, execution_time

# Define classifiers
classifiers = {
    "Decision Tree": DecisionTreeClassifier(featuresCol="features", labelCol="label"),
    "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="label"),
    "OneVsRest with GBT": OneVsRest(classifier=GBTClassifier(featuresCol="features", labelCol="label"))
}

# Evaluate each classifier
results = [evaluate_pipeline(clf, name) for name, clf in classifiers.items()]

# Print results
for name, accuracy, exec_time in results:
    print(f"{name} Accuracy: {accuracy:.4f}, Execution Time: {exec_time:.2f} seconds")

# Plot Accuracy and Execution Time
fig, ax1 = plt.subplots()
ax1.set_xlabel('Models')
ax1.set_ylabel('Accuracy', color='tab:blue')
ax1.bar([name for name, _, _ in results], [accuracy for _, accuracy, _ in results], color='tab:blue', alpha=0.6, label='Accuracy')
ax1.tick_params(axis='y', labelcolor='tab:blue')

ax2 = ax1.twinx()
ax2.set_ylabel('Execution Time (s)', color='tab:red')
ax2.plot([name for name, _, _ in results], [exec_time for _, _, exec_time in results], color='tab:red', marker='o', label='Execution Time')
ax2.tick_params(axis='y', labelcolor='tab:red')

fig.tight_layout()
plt.title("Model Accuracy vs Execution Time")
plt.show()

# Stop SparkContext
sc.stop()
