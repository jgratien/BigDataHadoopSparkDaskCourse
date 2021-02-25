import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkFiles
import os
import pandas as pd

sc =SparkContext()
sqlContext = SQLContext(sc)


data_dir="../data"
file = os.path.join(data_dir,"iris.csv")
panda_df = pd.read_csv(file)

sqlContext = SQLContext(sc)
df = sqlContext.read.csv("../data/iris.csv", header=True, inferSchema= True)	
#df=sqlContext.createDataFrame(panda_df)
df.printSchema()
#df.show(5, truncate = False)
df.select('petal_width','variety').show(5)

df.groupBy("variety").count().sort("count",ascending=True).show()

#df.describe().show()
sc.stop()
