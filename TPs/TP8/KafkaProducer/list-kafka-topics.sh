ROOT_DIR=`dirname $0`
. ${ROOT_DIR}/.properties

# List the topics
echo "Listing Kafka topics..."
${KAFKA_TOPICS} --list --zookeeper ${ZK_SERVER}
