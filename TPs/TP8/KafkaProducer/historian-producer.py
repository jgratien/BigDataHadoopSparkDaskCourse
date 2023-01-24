#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 17:15:37 2019

@author: gratienj
"""

import json

import time
import os
import pandas as pd

#import urllib.request
import requests
import json

url = 'http://sensors.ifpen.fr/'
def fromAPI(start,end,tags):
    # CALCULATED_DATA : interpolation from historian. It uses "interval" value
    mydata = {'server':'ISNTS35-N','type':'CALCULATED_DATA','tags':tags,'start':start,'end':end,'numberOfSamples':None,'qualifiedValues':True,"interval":600}
    # RAW_DATA : raw data. It uses "numberOfSample to limit output array size. If set to "none" retrieve all values between start and en dates. "interval" is not used but required
    mydata = {'server':'ISNTS35-N','type':'RAW_DATA','tags':tags,'start':start,'end':end,'numberOfSamples':1000,'qualifiedValues':True,"interval":None}
    s = json.dumps(mydata)
    r2 = requests.post(url, data=s)
    parsed = json.loads(r2._content)
    return parsed

from kafka import KafkaProducer


def main(start,end,tags):
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    raw_datas = fromAPI(start,end,tags)
    print("RAW DATA",json.dumps(raw_datas))
    tags = []
    topics = {}
    it=0
    for tag in raw_datas["headers"]:
        if it>0:
            new_tag = tag.replace(".","-")
            new_tag = new_tag.replace("_","-")
            tags.append(new_tag)
            topics[new_tag] = "historian-topic-"+new_tag
        it += 1
    index=0
    for data in raw_datas["values"]:
        #print("RAW DATA",data)
        topic_send_data = {}
        for tag in tags:
            topic_send_data[tag] = {}
        i = 0
        for d in data :
            if i == 0 :
                for tag in tags:
                    topic_send_data[tag]['date']=d
            else:
                print("D:",d)
                if d is None:
                    topic_send_data[tags[i-1]]['value']= 0
                else:
                    topic_send_data[tags[i-1]]['value']=d['value']
            i += 1
        tag = tags[1]
        print("TAG=",tag,topic_send_data[tag])
        producer.send(topics[tag],json.dumps(topic_send_data[tag]).encode())
        #for tag in tags:
        #    producer.send(topics[tag],json.dumps(topic_send_data[tag]).encode())
        #msg = json.dumps(send_data)
        print("{} Produced records[{}] topic : {}".format(time.time(), index,topics[tag]))
        time.sleep(0.1)
        index += 1
    
    
if __name__ == "__main__":
    start = '2019-09-03 00:00:00'
    end = '2019-09-03 18:00:00'
    tags = ['CRY.CENTRALE_SOLAIRE.CRY_act_prod_pow','CRY.TGBT_NORMAL.CRY_act_cons_pow','LAU.PHOTOVOLTAIQUE.LAU_app_prod_pow']
    main(start,end,tags)
