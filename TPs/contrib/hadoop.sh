module load java1.8
export HDUSER_HOME=$HOME/home/hduser
export JAVA_HOME=/softs/java/jdk1.8.0_121
export HADOOP_HOME=$HDUSER_HOME/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME 
export HADOOP_COMMON_HOME=$HADOOP_HOME 
export HADOOP_HDFS_HOME=$HADOOP_HOME 
export YARN_HOME=$HADOOP_HOME 

export PATH=$HADOOP_HOME/bin:$PATH
