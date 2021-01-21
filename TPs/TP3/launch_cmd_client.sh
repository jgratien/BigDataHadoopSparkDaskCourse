#/bin/sh
PYSPARK_DRIVER_PYTHON=/work/irlin355_1/gratienj/BigData/local/anaconda3/bin/python
PYSPARK_PYTHON=/work/irlin355_1/gratienj/BigData/local/anaconda3/bin/python
spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/work/irlin355_1/gratienj/BigData/local/anaconda3/bin/python \
--master yarn \
--deploy-mode client \
script.py
