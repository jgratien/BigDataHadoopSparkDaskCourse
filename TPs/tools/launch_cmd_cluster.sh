#/bin/sh
spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/work/irlin355_1/gratienj/BigData/local/anaconda3/bin/python \
--master yarn \
--deploy-mode cluster \
script.py
