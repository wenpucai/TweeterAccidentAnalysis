from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Word2Vec

from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline


def main(dict):
    filename = dict['filename']
    savedmodelName = dict['modelname']

    def myFunc(input):
        # print(input)
        lines = input.split("\n")
        for line in lines:
            parts=line.split(";")
            Category = parts[-1]
            Sentence = parts[1]
        return (Category,Sentence)


    # sc = SparkContext("local[2]", "ML")
    # spark = SparkSession.builder.master("local[2]").appName("ML").getOrCreate()
    file = sc.textFile("4CVTweets/"+filename)
    lines= file.map(myFunc)
    sentenceDataFrame = spark.createDataFrame(lines,["label","sentence"])
    (trainingData, testData) = sentenceDataFrame.randomSplit([0.7, 0.3])

    # start building the pineline

    # labelencoder = spark.createDataFrame(
    #             [(0, "NO"), (1, "crash"), (2, "fire"), (3, "shooting")],
    #             ["id", "label"])

    # No: 0,Crash:1,Fire:2,Shooting:3

    indexer = StringIndexer(inputCol="label", outputCol="categoryIndex")
    tokenizer = RegexTokenizer(pattern="\\w+",inputCol="sentence", outputCol="words",gaps=False)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=200)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    # # Compute the Inverse Document Frequency (IDF) given a collection of documents.

    rf = RandomForestClassifier(labelCol="categoryIndex", featuresCol="features", numTrees=100,maxDepth=10)
    # Using randomForest

    pipeline = Pipeline(stages=[indexer,tokenizer,remover, hashingTF, idf, rf])
    model = pipeline.fit(trainingData)

    # token = pipeline.getStages()[0]
    # remove = pipeline.getStages()[1]
    # hash = pipeline.getStages()[2]
    # myidf = pipeline.getStages()[3]
    # res = token.transform(trainingData)
    # res2 = remove.transform(res)
    # res3 = hash.transform(res2)
    # idfmodel = myidf.fit(res3)
    # res4 = idfmodel.transform(res3)
    # print(res4.show(100,False))

    # #
    # Start to count accuracy to evaluate the model using just the offline model

    predictionsForTraining = model.transform(trainingData)
    predictionsForTraining.select("label","categoryIndex","prediction").show(100,False)



    evaluator1 = MulticlassClassificationEvaluator(labelCol="categoryIndex", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator1.evaluate(predictionsForTraining)
    print("Train Error = %g " % (1.0 - accuracy))


    predictions = model.transform(testData)
    evaluator2 = MulticlassClassificationEvaluator(labelCol="categoryIndex", predictionCol="prediction", metricName="accuracy")

    accuracy = evaluator2.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))

    savePath = "/tmp/pipeline/"+savedmodelName
    model.write().overwrite().save(savePath)
    print("model for Location",savedmodelName,"save successfully.")
#

if __name__=='__main__':
    sc = SparkContext("local[2]", "ML")
    spark = SparkSession.builder.master("local[2]").appName("ML").getOrCreate()
    mydict = {'Boston':{'filename':"Boston4C.csv",'modelname':'boston'},
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