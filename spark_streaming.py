from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import json


sc = SparkContext("local[2]", "ML")
spark = SparkSession.builder.master("local[2]").appName("ML") \
.getOrCreate()
# Create a local StreamingContext with two working thread and batch interval of 1 second

def parse(str2):
    res ={}
    res['news']=[]
    coll = str2.collect()
    for each in coll:
        d = {}
        d['label']= each['label']
        d['prediction'] =each['prediction']
        res['news'].append(d)
    return res;

def result(x):

    sl = x.map(lambda line: (line[0],line[1].split(";"))).map(lambda line:(line[0],line[1][-1],line[1][1]))
    # get the Topic (location), Incident type, and Sentence

    sentenceDataFrame = spark.createDataFrame(sl, ["topic","label", "sentence"])
    collections = sentenceDataFrame.collect()
    topic = collections[0][0]
    # Get the topic of this RDD(Assume the same RDD has the same Topic)


    model = PipelineModel.load("/tmp/pipeline/" + topic)
    predictionsForTraining = model.transform(sentenceDataFrame)
    predictionsForTraining.select("topic","label", "categoryIndex", "prediction").show(100, False)

    evaluator1 = MulticlassClassificationEvaluator(labelCol="categoryIndex", predictionCol="prediction",
                                                   metricName="accuracy")
    accuracy = evaluator1.evaluate(predictionsForTraining)
    print("Train Error = %g " % (1.0 - accuracy))




es = Elasticsearch()
es_write_conf = {"es.resource": "tsd/mem", "es.input.json": "true"}


ssc = StreamingContext(sc, 5)


lines = KafkaUtils.createDirectStream(ssc,['Boston4C','Brisbane4C','Chicago4C','Dublin4C','London4C','Memphis4C','NYC4C','SanFrancisco4Classes','Seattle4Classes','Sydney4C'],{"bootstrap.servers":"localhost:9092"})

lines.foreachRDD(result)
# For each RDD, call result

lines.pprint()
ssc.start()
ssc.awaitTermination()


# /Users/yizheng/Desktop/BigDataProj/spark_streaming.py>> /Users/yizheng/Desktop/BigDataProj/output.txt