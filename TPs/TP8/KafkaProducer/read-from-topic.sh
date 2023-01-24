#!/bin/sh
ROOT_DIR=`dirname $0`
. ${ROOT_DIR}/.properties

if [ "$#" -ne 1 ]; then
  echo "Script to display data in a topic"
  echo "Usage: $0 <topic>" >&2
  exit 1
fi
TOPIC=$1

# List the topics
echo "Displaying content of topic ${TOPIC}..."
${KAFKA_CONSUMER} --bootstrap-server ${KAFKA_BROKER} --from-beginning --topic ${TOPIC} --consumer.config kafka_client.properties
#${KAFKA_CONSUMER} --bootstrap-server ${KAFKA_BROKER} --topic ${TOPIC} --consumer.config kafka_client.properties

