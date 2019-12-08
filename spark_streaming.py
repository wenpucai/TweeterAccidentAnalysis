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


def result(x, es):
    if not x.isEmpty():
        sl = x.map(lambda line: (line[0],line[1].split(";"))).map(lambda line:(line[0],line[1][-1],line[1][1]))
        sentenceDataFrame = spark.createDataFrame(sl, ["topic","label", "sentence"])
        collections = sentenceDataFrame.collect()
        topic = collections[0][0]
        # Get the topic of this RDD(Assume the same RDD has the same Topic)

        model = PipelineModel.load("/tmp/pipeline/" + topic)
        # sentenceDataFrame.show(10,False)
        predictionsForTraining = model.transform(sentenceDataFrame)

        joindf = spark.createDataFrame(
            [(0.0, "NO"), (1.0, "crash"), (2.0, "fire"), (3.0, "shooting")],
            ["prediction", "Predictlabel"])
        innerjoin = predictionsForTraining.join(joindf, joindf.prediction == predictionsForTraining.prediction).drop(
            joindf.prediction)

        # innerjoin.select("label","categoryIndex","prediction","Predictlabel").show(1000,False)
        #innerjoin.select("topic","sentence","prediction", "Predictlabel").show(1000, False)
        ans = innerjoin.select("topic", "sentence","Predictlabel")
        mm = ans.rdd.collect();
        for m in mm:
            print("haha")
            print(m[0], m[2])
            es.index(index='tweeter_accidents', doc_type='_doc', id=hash(m[1]),body={
                "location" : str(m[0]),
                "content" : str(m[2])})






    return


es = Elasticsearch([{'host':'localhost', 'port':9200}])


ssc = StreamingContext(sc, 5)
lines = KafkaUtils.createDirectStream(ssc,['Boston4C','Brisbane4C','Chicago4C','Dublin4C','London4C','Memphis4C','NYC4C','SanFrancisco4Classes','Seattle4Classes','Sydney4C'],{"bootstrap.servers":"localhost:9092"})

lines.foreachRDD(lambda x : result(x, es))

# For each RDD, call result

lines.pprint()
ssc.start()
ssc.awaitTermination()


# /Users/yizheng/Desktop/BigDataProj/spark_streaming.py>> /Users/yizheng/Desktop/BigDataProj/output.txt