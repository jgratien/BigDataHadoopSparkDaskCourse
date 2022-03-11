from dask.distributed import Client
import joblib

from sklearn.datasets import load_iris
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

import pandas as pd
import numpy as np
from pathlib import Path

PATH = Path().absolute() / "data"

classifiers = [
    DecisionTreeClassifier(max_depth=4),
    RandomForestClassifier(max_depth=4),
    GradientBoostingClassifier(max_depth=4),
]

df = pd.read_csv(PATH / "iris.csv")

df["ind_variety"] = 0
for i, value in enumerate(list(dict.fromkeys(df["variety"].values))):
    df.loc[df["variety"] == value, "ind_variety"] = i


attribs = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
X = np.array(df[attribs])
y = np.array(df["ind_variety"])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

client = Client(processes=False)

for i in range(len(classifiers)):
    with joblib.parallel_backend("dask"):
        classifiers[i].fit(X_train, y_train)
        y_pred = classifiers[i].predict(X_test)
        print("For the following classifier :", classifiers[i])
        print("Accuracy : ", accuracy_score(y_test, y_pred))
