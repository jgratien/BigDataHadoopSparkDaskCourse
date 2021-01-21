sh $HADOOP_HOME/sbin/start-all.sh
sh $SPARK_HOME/sbin/start-master.sh
sh $SPARK_HOME/sbin/start-slave.sh spark://localhost:7077
