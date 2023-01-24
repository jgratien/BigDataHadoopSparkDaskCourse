#!/bin/sh
ROOT_DIR=`dirname $0`
. ${ROOT_DIR}/.properties

if [ "$#" -ne 1 ]; then
  echo "Inject NASA log file into a topic"
  echo "Usage: $0 <topic>" >&2
  exit 1
fi
TOPIC=$1

echo "Creating topic ${TOPIC} (please ignore error if already exist)..."
#${KAFKA_TOPICS} --create --zookeeper ${ZK_SERVER} --replication-factor 3 --partitions 3 --topic ${TOPIC}

# Inject the logs into the topic
TEXT=`date`
echo "Writing ${TEXT} into topic ${TOPIC}..."
echo "${TEXT}" | ${KAFKA_PRODUCER} --broker-list ${KAFKA_BROKER} --topic ${TOPIC} --producer.config kafka_client.properties
