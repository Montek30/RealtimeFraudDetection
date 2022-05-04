from kafka import KafkaProducer
import json
import csv
import time


class DataGenerator:
    def getCSVData(self):
        try:
            producer = Producer()
            with open('/home/admi/Downloads/creditcard.csv', 'r') as csvfile:
                reader=csv.reader(csvfile)
                j = 0
                for row in reader:
                    if j == 0:
                        j += 1
                        continue
                    
                    send_data = {"V1":row[1],"V2":row[2],"V3":row[3],"V4":row[4],"V5":row[5],"V6":row[6],"V7":row[7],
                    "V8":row[8],"V9":row[9],"V10":row[10],"V11":row[11],"V12":row[12],"V13":row[13],"V14":row[14],"V15":row[15],
                    "V16":row[16],"V17":row[17],"V18":row[18],"V19":row[19],"V20":row[20],"V21":row[21],"V22":row[22],"V23":row[23],
                    "V24":row[24],"V25":row[25],"V26":row[26],"V27":row[27],"V28":row[28],"Class":row[30],"Email":row[31]}

                    producer.sendData(send_data, "topicCase22")

                    time.sleep(10)   
        except KeyboardInterrupt:
            exit()


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(
                                        bootstrap_servers=['localhost:9092','localhost:9093'],
                                        api_version=(0,11,5), 
                                        key_serializer=lambda a:json.dumps(a).encode('utf-8'), value_serializer=lambda b:json.dumps(b).encode('utf-8')
                                    )

        #in order to publish to different partitions
        self.hash = 0

    def sendData(self ,data, topic):
        data=json.dumps(data)
        jsondata=json.loads(data)
        
        print("----")
        print(jsondata)
        print("ttl", self.hash)
        print("----")

        self.hash += 1

        #case 1
        # future=self.producer.send(topic, value=jsondata)
        
        #case 2
        future=self.producer.send(topic, value=jsondata, key = self.hash%2)
        
        #case 3
        # future=self.producer.send(topic, value=jsondata, key = self.hash%3)
        self.producer.flush()

dataGenerator=DataGenerator()

try:
    while True:
        temp_data=dataGenerator.getCSVData()
except KeyboardInterrupt:
    exit()