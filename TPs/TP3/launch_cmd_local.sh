#/bin/sh
PYSPARK_DRIVER_PYTHON=/home/gext/abdessalam.benhari/hduser/local/anaconda33/bin/python
PYSPARK_PYTHON=/home/gext/abdessalam.benhari/hduser/local/anaconda33/bin/python
spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/home/gext/abdessalam.benhari/hduser/local/anaconda33/bin/python \
--master local[*] \
--deploy-mode client \
$1
