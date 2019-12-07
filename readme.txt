Start zookeeper server:
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

Start the Kafka:
kafka-server-start /usr/local/etc/kafka/server.properties

run trainingOffline.py to train model and save:
python3 trainingOffline.py (Remember to set the dataset as Directory "4CVTweets")

run Kafka_Producer.py:
python3 Kafka_Producer.py (Remember to set the dataset as Directory "4CVTweets")

run spark_streaming.py:

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark_streaming.py(path of spark_streaming.py) >> outputfile

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 /Users/yizheng/Desktop/BigDataProj/spark_streaming.py>> /Users/yizheng/Desktop/BigDataProj/output.txt 