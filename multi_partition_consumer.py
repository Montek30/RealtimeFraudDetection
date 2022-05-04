from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.ml import PipelineModel

from email.header    import Header
from email.mime.text import MIMEText
from getpass         import getpass
from smtplib         import SMTP_SSL

from pymongo import MongoClient

def send_mail(to_email):
	login, password = 'monteks764@gmail.com', "heyhey123@H"
	recipients = [to_email]

	msg = MIMEText('Alert! Fraudulent Transaction has been detected from your carf', 'plain', 'utf-8')
	msg['Subject'] = "Fraudulent Transaction!!!!!"
	msg['From'] = login
	msg['To'] = ", ".join(recipients)

	s = SMTP_SSL('smtp.gmail.com', 465, timeout=10)
	
	try:
	    s.login(login, password)
	    s.sendmail(msg['From'], recipients, msg.as_string())
	finally:
	    s.quit()


#creating the mongodb connection
client = MongoClient(port=27017, connect = False)
db=client.credit_card_fraud_data

print("Successfully connected to Mongo")


#creating spark connection
spark = SparkSession.builder.master("local[*]").appName('Logistic_Regression').getOrCreate()
sc = SparkContext.getOrCreate()

print("Successfully created Spark Session")


#Loading the ML Model
dt2 = PipelineModel.load("/var/www/html/credit_card_fraud_detection_model/model")

print("Successfully Loaded the model")


#Kafka Consumer
consumer = KafkaConsumer('topicCase22', api_version=(0,11,5), 
										auto_offset_reset='earliest',
										group_id='consumer_gr_1',
										bootstrap_servers=['localhost:9092','localhost:9093']
						)

print("Successfully created the kafka consumer")

partitions = consumer.partitions_for_topic('topicCase22')
print(partitions)

print("Starting consumer")
k = 0
for msg in consumer:
	msg=msg.value.decode("utf-8")
	df = spark.read.json(sc.parallelize([msg]))
	df=df.withColumn('V1',df['V1'].cast("double").alias('V1'))
	df=df.withColumn('V2',df['V2'].cast("double").alias('V2'))
	df=df.withColumn('V3',df['V3'].cast("double").alias('V3'))
	df=df.withColumn('V4',df['V4'].cast("double").alias('V4'))
	df=df.withColumn('V5',df['V5'].cast("double").alias('V5'))
	df=df.withColumn('V6',df['V6'].cast("double").alias('V6'))
	df=df.withColumn('V7',df['V7'].cast("double").alias('V7'))
	df=df.withColumn('V8',df['V8'].cast("double").alias('V8'))
	df=df.withColumn('V9',df['V9'].cast("double").alias('V9'))
	df=df.withColumn('V10',df['V10'].cast("double").alias('V10'))
	df=df.withColumn('V11',df['V11'].cast("double").alias('V11'))
	df=df.withColumn('V12',df['V12'].cast("double").alias('V12'))
	df=df.withColumn('V13',df['V13'].cast("double").alias('V13'))
	df=df.withColumn('V14',df['V14'].cast("double").alias('V14'))
	df=df.withColumn('V15',df['V15'].cast("double").alias('V15'))
	df=df.withColumn('V16',df['V16'].cast("double").alias('V16'))
	df=df.withColumn('V17',df['V17'].cast("double").alias('V17'))
	df=df.withColumn('V18',df['V18'].cast("double").alias('V18'))
	df=df.withColumn('V19',df['V19'].cast("double").alias('V19'))
	df=df.withColumn('V20',df['V20'].cast("double").alias('V20'))
	df=df.withColumn('V21',df['V21'].cast("double").alias('V21'))
	df=df.withColumn('V22',df['V22'].cast("double").alias('V22'))
	df=df.withColumn('V23',df['V23'].cast("double").alias('V23'))
	df=df.withColumn('V24',df['V24'].cast("double").alias('V24'))
	df=df.withColumn('V25',df['V25'].cast("double").alias('V25'))
	df=df.withColumn('V26',df['V26'].cast("double").alias('V26'))
	df=df.withColumn('V27',df['V27'].cast("double").alias('V27'))
	df=df.withColumn('V28',df['V28'].cast("double").alias('V28'))
	
	email_address = df.first()["Email"]
	
	print(email_address)
	
	pred = dt2.transform(df)
	pred.select("prediction").show()
	va = pred.select("prediction")
	
	email_sent = 0
	
	if int(va.first()[0]) == 1:
		email_sent = 1
		send_mail(email_address)

	mongo_data = {
	'email' : email_address,
	'fraud_detected' : int(va.first()[0]),
	'email_sent' : email_sent
	}

	result=db.data.insert_one(mongo_data)
	
	df.select("Class").show()
	print(msg,"-----------------")
	k += 1
	print(k)
