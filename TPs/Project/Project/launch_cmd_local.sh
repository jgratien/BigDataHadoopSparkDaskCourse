#/bin/sh
PYSPARK_DRIVER_PYTHON=/home/ouadie/anaconda3/bin/python
PYSPARK_PYTHON=/home/ouadie/anaconda3/bin/python
spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/home/ouadie/anaconda3/bin/python \
--master local[*] \
--deploy-mode local \
$1
