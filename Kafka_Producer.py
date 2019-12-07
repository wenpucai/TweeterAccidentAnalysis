import json
import time
import glob
import csv
from kafka import KafkaProducer


def getData(path):
    data = []
    with open(path, mode='r', encoding='ISO-8859-1') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\n')
        for row in csv_reader:
            if row is not None:
                str1 = ''.join(row)
                data.append(str1)
    return data


def publish_message(producer_instance, topic_name, value):
    try:
        mydict = {'Boston4C': "Boston",
                  'Brisbane4C': "Brisbane",
                  'Chicago4C': "Chicago",
                  'Dublin4C': "Dublin",
                  'London4C': "London",
                  'Memphis4C': "Memphis",
                  'NYC4C': "New York",
                  "SanFrancisco4Classes": 'SanFrancisco',
                  "Seattle4Classes": 'Seattle',
                  'Sydney4C': "Sydney",
                  }

        key_bytes = bytes(mydict[topic_name], encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        # _producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def findFileName(pathNameList):
    fileNameList = []
    for pathName in pathNameList:
        fileName = pathName.split('/')[-1]
        fileName = fileName.split('.')[0]
        fileNameList.append(fileName)
    return fileNameList


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print('Number of arguments is not correct')
    #     exit()

    topicName = ''

    path = '4CVTweets'
    # path = sys.argv[1]

    pathNames = glob.glob(path +'/*.csv')
    fileNameList = findFileName(pathNames)

    for count, path in enumerate(pathNames):
        city_data = getData(path)
        topic = fileNameList[count]
        if len(city_data) > 0:
            prod = connect_kafka_producer();
            for item in city_data:
                print(json.dumps(item))
                publish_message(prod, topic, item)
                time.sleep(1)
            if prod is not None:
                prod.close()

