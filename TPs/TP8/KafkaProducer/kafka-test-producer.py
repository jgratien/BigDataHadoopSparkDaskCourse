#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 17:15:37 2019

@author: gratienj
"""

import json

import time
import os
import sys

import pandas as pd


#from kafka import KafkaProducer
from confluent_kafka import Producer


def main(folder,file,kafka_server):
    #producer = KafkaProducer(bootstrap_servers=kafka_server)
    #producer = KafkaProducer(security_protocol="SASL_PLAINTEXT",sasl_mechanism='GSSAPI',sasl_kerberos_service_name='kafka',bootstrap_servers=kafka_server)
    conf = {'bootstrap.servers':kafka_server,
            'security.protocol':'SASL_PLAINTEXT',
            'sasl.mechanism':'GSSAPI',
            'sasl.kerberos.service.name':'kafka',
            'sasl.kerberos.principal':'gratienj@HADOOP.IFP.FR',
            'sasl.kerberos.keytab':'/home/irsrvhome1/R11/gratienj/gratienj.keytab'}
    producer = Producer(**conf)
    #with open(os.path.join(data_dir,'Output','output_training.csv'),'r') as f:
    data_file=os.path.join(folder,file)

    data_df=pd.read_csv(data_file,sep=' ',header=None)
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
    while True:
        for index,row in data_df.iterrows():
            count=str(row[0])
            value=row[1]
            msg = count+":"+value
            try:
                print('Try to send msg:',msg)
                #producer.send("mytopic",msg.encode('utf8'))
                producer.produce("mytopic",msg.encode('utf8'),callback=delivery_callback)
                print("{} Produced records[{}] : {}".format(time.time(), index,msg))
                time.sleep(1)
            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(producer))
            producer.poll(0)

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
        producer.flush()
    
    
if __name__=="__main__":
    data_folder='/home/irsrvshare2/R11/dgt_sandb/data/TimeSeries'
    test='Test1.csv'    
    kafka_server='islin-hdpnod1:6667'
    kafka_server='islin-hdplnod06.ifp.fr:6667'
    main(data_folder,test,kafka_server)
