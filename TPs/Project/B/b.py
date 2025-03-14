import dask.dataframe as dd
from dask_ml.preprocessing import LabelEncoder, StandardScaler
from dask_ml.model_selection import train_test_split, GridSearchCV
from dask_ml.wrappers import ParallelPostFit
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score
from dask.distributed import Client

# Initialize Dask client
client = Client()

# Load the dataset into a Dask DataFrame
df = dd.read_csv('iris.csv')

# Encode the target variable
label_encoder = LabelEncoder()
df['variety'] = label_encoder.fit_transform(df['variety'])

# Define features and target
X = df[['sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
y = df['variety']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

# Define a function to create and evaluate a model pipeline
def evaluate_model(model, model_name):
    # Create a pipeline with scaling, dimensionality reduction, and the classifier
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', model)
    ])

    # Wrap the pipeline with ParallelPostFit for parallel prediction
    parallel_pipeline = ParallelPostFit(estimator=pipeline)

    # Train the model
    parallel_pipeline.fit(X_train, y_train)

    # Make predictions
    y_pred = parallel_pipeline.predict(X_test)

    # Evaluate accuracy
    accuracy = accuracy_score(y_test, y_pred)
    print(f'{model_name} Accuracy: {accuracy:.2f}')

# Evaluate Decision Tree Classifier
evaluate_model(DecisionTreeClassifier(max_depth=4), 'Decision Tree Classifier')

# Evaluate Random Forest Classifier
evaluate_model(RandomForestClassifier(n_estimators=100), 'Random Forest Classifier')

# Evaluate Gradient Boosting Classifier
evaluate_model(GradientBoostingClassifier(n_estimators=100), 'Gradient Boosting Classifier')

# Close Dask client
client.close()
