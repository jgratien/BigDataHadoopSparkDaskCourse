import pyspark
from pyspark import SparkContext

sc =SparkContext.getOrCreate()
nums= sc.parallelize([1,2,3,4])
print("Results : ", nums.collect())
nums.take(1)
sc.stop()
