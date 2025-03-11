#/bin/sh
PYSPARK_DRIVER_PYTHON=/gext/jean-marc.gratien/home/hduser/local/anaconda3/bin/python
PYSPARK_PYTHON=/gext/jean-marc.gratien/home/hduser/local/anaconda3/bin/python
spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/gext/jean-marc.gratien/home/hduser/local/anaconda3/bin/python \
--master local[*] \
--deploy-mode client \
$1
