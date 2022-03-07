from pyspark.sql import SparkSession

from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.ml.classification import DecisionTreeClassifier


spark = SparkSession.builder.master("local[*]").appName('Logistic_Regression').getOrCreate()

decision_tree_df = spark.read.option("header", "true").csv("/home/admi/Downloads/creditcard.csv")

decision_tree_df=decision_tree_df.withColumn('V1',decision_tree_df['V1'].cast("double").alias('V1'))
decision_tree_df=decision_tree_df.withColumn('V2',decision_tree_df['V2'].cast("double").alias('V2'))
decision_tree_df=decision_tree_df.withColumn('V3',decision_tree_df['V3'].cast("double").alias('V3'))
decision_tree_df=decision_tree_df.withColumn('V4',decision_tree_df['V4'].cast("double").alias('V4'))
decision_tree_df=decision_tree_df.withColumn('V5',decision_tree_df['V5'].cast("double").alias('V5'))
decision_tree_df=decision_tree_df.withColumn('V6',decision_tree_df['V6'].cast("double").alias('V6'))
decision_tree_df=decision_tree_df.withColumn('V7',decision_tree_df['V7'].cast("double").alias('V7'))
decision_tree_df=decision_tree_df.withColumn('V8',decision_tree_df['V8'].cast("double").alias('V8'))
decision_tree_df=decision_tree_df.withColumn('V9',decision_tree_df['V9'].cast("double").alias('V9'))
decision_tree_df=decision_tree_df.withColumn('V10',decision_tree_df['V10'].cast("double").alias('V10'))
decision_tree_df=decision_tree_df.withColumn('V11',decision_tree_df['V11'].cast("double").alias('V11'))
decision_tree_df=decision_tree_df.withColumn('V12',decision_tree_df['V12'].cast("double").alias('V12'))
decision_tree_df=decision_tree_df.withColumn('V13',decision_tree_df['V13'].cast("double").alias('V13'))
decision_tree_df=decision_tree_df.withColumn('V14',decision_tree_df['V14'].cast("double").alias('V14'))
decision_tree_df=decision_tree_df.withColumn('V15',decision_tree_df['V15'].cast("double").alias('V15'))
decision_tree_df=decision_tree_df.withColumn('V16',decision_tree_df['V16'].cast("double").alias('V16'))
decision_tree_df=decision_tree_df.withColumn('V17',decision_tree_df['V17'].cast("double").alias('V17'))
decision_tree_df=decision_tree_df.withColumn('V18',decision_tree_df['V18'].cast("double").alias('V18'))
decision_tree_df=decision_tree_df.withColumn('V19',decision_tree_df['V19'].cast("double").alias('V19'))
decision_tree_df=decision_tree_df.withColumn('V20',decision_tree_df['V20'].cast("double").alias('V20'))
decision_tree_df=decision_tree_df.withColumn('V21',decision_tree_df['V21'].cast("double").alias('V21'))
decision_tree_df=decision_tree_df.withColumn('V22',decision_tree_df['V22'].cast("double").alias('V22'))
decision_tree_df=decision_tree_df.withColumn('V23',decision_tree_df['V23'].cast("double").alias('V23'))
decision_tree_df=decision_tree_df.withColumn('V24',decision_tree_df['V24'].cast("double").alias('V24'))
decision_tree_df=decision_tree_df.withColumn('V25',decision_tree_df['V25'].cast("double").alias('V25'))
decision_tree_df=decision_tree_df.withColumn('V26',decision_tree_df['V26'].cast("double").alias('V26'))
decision_tree_df=decision_tree_df.withColumn('V27',decision_tree_df['V27'].cast("double").alias('V27'))
decision_tree_df=decision_tree_df.withColumn('V28',decision_tree_df['V28'].cast("double").alias('V28'))

#splitting data into train and test
decision_tree_df = decision_tree_df.randomSplit([0.7, 0.3], 1234)
decision_tree_train_df = decision_tree_df[0]
decision_tree_test_df = decision_tree_df[1]

#vector assemblar is a feature assembler which combines the input features into a single column vector
#inputCols is the input features that are provided to the vector assemblar
#outputCol is the output feature variable which contains the vector form of the input features
#handleInvalid is an extra parameter which when set to 'skip' will eliminate the rows that have invalid data  
decision_tree_features_vector = VectorAssembler(inputCols=["V1","V2","V3","V4","V5","V6","V7","V8","V9","V10","V11","V12","V13","V14","V15","V16","V17","V18","V19","V20","V21","V22","V23","V24","V25","V26","V27","V28"], outputCol="decision_tree_model_features", handleInvalid='skip')


#string indexer is a mapper which maps the string data type columns to the indices.
#The indices range from 0 to no_of_labels-1
#we can also manipulate the ordering of the indices with an extra parameter passed.
#handleInvalid is an extra parameter which when set to 'skip' will eliminate the rows that have invalid data
decision_tree_label_indexer = StringIndexer(inputCol="Class", outputCol="decision_tree_model_label", handleInvalid='skip') #remove handleinvalid if getting error

#Decision tree classifier takes two parameters : labelCol->the label to be classified, featuresCol->vector of the features
decision_tree = DecisionTreeClassifier(labelCol="decision_tree_model_label", featuresCol="decision_tree_model_features")

# Machine Learning pipeline with 3 stages to be run in a specific order.
pipeline = Pipeline(stages=[decision_tree_label_indexer, decision_tree_features_vector, decision_tree])

# Run the three stages in pipeline and train the decision tree model
model = pipeline.fit(decision_tree_train_df)


model.save("/var/www/html/credit_card_fraud_detection_model/model")

#This function is used on the testing data that has been trained on training data to get the predictions. We can determine the performance of the model based on this.
predictions = model.transform(decision_tree_test_df)

# schema of the predictions
predictions.printSchema()

# Select 5 predictions to show
predictions.select("prediction", "decision_tree_model_label", "decision_tree_model_features").show(5)

#evaluator to perform the analysis and evaluate the metrics.
decision_tree_model_evaluator = MulticlassClassificationEvaluator(labelCol="decision_tree_model_label", predictionCol="prediction", metricName="accuracy")
accuracy = decision_tree_model_evaluator.evaluate(predictions)
print("Model Accuracy: ", accuracy) 

