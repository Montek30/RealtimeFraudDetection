from kafka import KafkaConsumer

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.ml import PipelineModel


spark = SparkSession.builder.master("local[*]").appName('Logistic_Regression').getOrCreate()
sc = SparkContext.getOrCreate()

dt2 = PipelineModel.load("/var/www/html/credit_card_fraud_detection_model/model")

print("Successfully Loaded the model")

consumer = KafkaConsumer('fifth_topic_2',group_id='my-tenth-application',bootstrap_servers=['localhost:9092'], api_version=(0,11,5))

print("Successfully created the kafka consumer")


i=0
for msg in consumer:
   	if i==0:
   		i+=1
   		continue
   	
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
   	
   	pred = dt2.transform(df)
   	
   	pred.select("prediction").show()
   	print("---------")
   	df.select("Class").show()

   	i += 1