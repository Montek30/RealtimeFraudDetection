from kafka import KafkaProducer
import json
import random
import datetime
import uuid
import sys
import csv
import time


class DataGenerator:
    def __init__(self):
        self.userList = []
        self.getUserDetails()

    def getUserDetails(self):
        try:
            myListener = MyListener()
            with open('/home/admi/Downloads/creditcard.csv', 'r') as csvfile:
                reader=csv.reader(csvfile)
                for row in reader:
                    # send_data = {"V1":"","V2":"","V3":"","V4":"","V5":"","V6":"","V7":"",
                    # "V8":"","V9":"","V10":"","V11":"","V12":"","V13":"","V14":"","V15":"",
                    # "V16":"","V17":"","V18":"","V19":"","V20":"","V21":"","V22":"","V23":"",
                    # "V24":"","V25":"","V26":"","V27":"","V28":"","Class":""}

                    send_data = {"V1":row[1],"V2":row[2],"V3":row[3],"V4":row[4],"V5":row[5],"V6":row[6],"V7":row[7],
                    "V8":row[8],"V9":row[9],"V10":row[10],"V11":row[11],"V12":row[12],"V13":row[13],"V14":row[14],"V15":row[15],
                    "V16":row[16],"V17":row[17],"V18":row[18],"V19":row[19],"V20":row[20],"V21":row[21],"V22":row[22],"V23":row[23],
                    "V24":row[24],"V25":row[25],"V26":row[26],"V27":row[27],"V28":row[28],"Class":row[30],"Email":row[31]}

                    myListener.send_data(send_data)
                    time.sleep(10)   
        except KeyboardInterrupt:
            exit()


class MyListener:
    def __init__(self):
        # self.producerObj = Producer()
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,11,5),
            client_id="test-producer",acks=1,retries=5, 
            key_serializer=lambda a:json.dumps(a).encode('utf-8'),value_serializer=lambda b:json.dumps(b).encode('utf-8'))
        # print (self.producer.partitions_for('fifth_topic'))
        # print (self.producerObj)

    def send_data(self,data):
        data=json.dumps(data)
        jsondata=json.loads(data)
        
        print("----")
        print(jsondata)
        print("----")
        
        future=self.producer.send('fifth_topic_3', value=jsondata, key="AAA")

        self.producer.flush()

dataGenerator=DataGenerator()

try:
    while True:
        temp_data=dataGenerator.getUserDetails()
except KeyboardInterrupt:

    exit()