from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import StringIndexer,IndexToString
from pyspark.ml.feature import Word2Vec

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
import re


def main(dict):

    filename = dict['filename']
    savedmodelName = dict['modelname']

    def myFunc(input):
        lines = input.split("\n")
        for line in lines:
            parts=line.split(";")
            Category = parts[-1]
            Sentence = parts[1]
            url_pattern = re.compile(r'(http[s]://[\w./]+)*')
            rt_pattern = re.compile('RT @\w+: ')
            r_pattern = re.compile('@\w+ ')
            Sentence =  r_pattern.sub(r'', rt_pattern.sub(r'', url_pattern.sub(r'', Sentence))).replace('\n',' ').strip()
        return (Category,Sentence)

    file = sc.textFile("4CVTweets/"+filename)
    lines= file.map(myFunc)
    sentenceDataFrame = spark.createDataFrame(lines,["label","sentence"])
    (trainingData, testData) = sentenceDataFrame.randomSplit([0.7, 0.3])
    df = spark.createDataFrame(
        [(0, "NO"), (1, "crash"), (2, "fire"), (3, "shooting")],
        ["id", "label"])

    # start building the pineline
    # No: 0,Crash:1,Fire:2,Shooting:3

    indexer = StringIndexer(inputCol="label", outputCol="categoryIndex")
    indexer.fit(df)

    tokenizer = RegexTokenizer(pattern="\\w+",inputCol="sentence", outputCol="words",gaps=False)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features",minDocFreq=5)

    # # Compute the Inverse Document Frequency (IDF) given a collection of documents.

    rf = RandomForestClassifier(labelCol="categoryIndex", featuresCol="features", numTrees=100,maxDepth=10)

    # Using randomForest
    # mlr = LogisticRegression(maxIter=100, regParam=0.3, elasticNetParam=0.8, family="multinomial",featuresCol="features",labelCol="categoryIndex")
    # Naive Bayers
    nb = NaiveBayes(labelCol="categoryIndex", featuresCol="features", smoothing=1)

    # converter = IndexToString(inputCol="prediction", outputCol="originalCategory")
    pipeline = Pipeline(stages=[indexer,tokenizer,remover, hashingTF, idf,nb])
    model = pipeline.fit(trainingData)


    # Start to count accuracy to evaluate the model using just the offline model

    predictionsForTraining = model.transform(trainingData)

    predictionsForTraining.show(100,False)

    joindf = spark.createDataFrame(
        [(0.0, "NO"), (1.0, "crash"), (2.0, "fire"), (3.0, "shooting")],
        ["prediction", "Predictlabel"])
    innerjoin = predictionsForTraining.join(joindf,joindf.prediction==predictionsForTraining.prediction).drop(joindf.prediction)

    # innerjoin.select("label","categoryIndex","prediction","Predictlabel").show(1000,False)
    innerjoin.select("label", "Predictlabel").show(1000, False)



    evaluator1 = MulticlassClassificationEvaluator(labelCol="categoryIndex", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator1.evaluate(predictionsForTraining)
    print("Test Accuracy = %g " % (accuracy))
    print("Train Error = %g " % (1.0 - accuracy))



    predictions = model.transform(testData)
    evaluator2 = MulticlassClassificationEvaluator(labelCol="categoryIndex", predictionCol="prediction", metricName="accuracy")

    accuracy = evaluator2.evaluate(predictions)
    print("Test Accuracy = %g " % (accuracy))
    print("Test Error = %g " % (1.0 - accuracy))

    savePath = "/tmp/pipeline/"+savedmodelName
    model.write().overwrite().save(savePath)
    print("model for Location",savedmodelName,"save successfully.")
#

if __name__=='__main__':
    sc = SparkContext("local[2]", "ML")
    spark = SparkSession.builder.master("local[2]").appName("ML").getOrCreate()
    mydict = {'Boston':{'filename':"Boston4C.csv",'modelname':'Boston'},
              'Brisbane': {'filename': "Brisbane4C.csv", 'modelname': 'Brisbane'},
              'Chicago':{'filename': "Chicago4C.csv", 'modelname': 'Chicago'},
              'Dublin': {'filename': "Dublin4C.csv", 'modelname': 'Dublin'},
              'London': {'filename': "London4C.csv", 'modelname': 'London'},
              'Memphis': {'filename': "Memphis4C.csv", 'modelname': 'Memphis'},
              'New York': {'filename': "NYC4C.csv", 'modelname': 'New York'},
              'SanFrancisco':{'filename': "SanFrancisco4Classes.csv", 'modelname': 'SanFrancisco'},
              'Seattle': {'filename': "Seattle4Classes.csv", 'modelname': 'Seattle'},
              'Sydney': {'filename': "Sydney4C.csv", 'modelname': 'Sydney'},
              }
    for key,value in mydict.items():
        print("For Location",key, "Data:")
        main(value)
    # main()