#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 21:33:58 2019

@author: gratienj
"""

import sys
import json

#from kafka import KafkaConsumer
from confluent_kafka import Consumer, KafkaException
import logging
from pprint import pformat

def main(kafka_server,topic):

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    #consumer = KafkaConsumer(topic, bootstrap_servers=kafka_server)
    #consumer = KafkaConsumer(topic, bootstrap_servers=kafka_server,security_protocol="SASL_PLAINTEXT",sasl_mechanism='GSSAPI',sasl_kerberos_service_name='kafka')
    group = 0
    conf = {'bootstrap.servers':kafka_server,
            'group.id': group, 
            'session.timeout.ms': 6000,
            'security.protocol':'SASL_PLAINTEXT',
            'sasl.mechanism':'GSSAPI',
            'sasl.kerberos.service.name':'kafka',
            'sasl.kerberos.principal':'gratienj@HADOOP.IFP.FR',
            'sasl.kerberos.keytab':'/home/irsrvhome1/R11/gratienj/gratienj.keytab'}
    consumer = Consumer(**conf,logger=logger)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    topics = ['mytopic']
    consumer.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


    # group_id="velib-monitor-stations
    '''
    for message in consumer:
        msg = message.value.decode()
        print('RECEIVED MSG',msg)
    '''
        
if __name__=="__main__":
    kafka_server='localhost:9092'
    kafka_server='islin-hdpnod1:6667'
    kafka_server='islin-hdplnod06:6667'
    topic = 'mytopic'
    main(kafka_server,topic)
