import pyspark
from pyspark import SparkContext
sc =SparkContext()
nums= sc.parallelize([1,2,3,4])	
nums.take(1)
