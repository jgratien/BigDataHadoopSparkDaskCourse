spark-submit \
--master    spark://front-in1.cemef:7077 \
--conf      spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
$1