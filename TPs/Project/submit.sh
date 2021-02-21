spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
--master spark://front-in1.cemef:7077 \
--deploy-mode cluster \
$1