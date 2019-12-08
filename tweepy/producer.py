# -*- coding: utf-8 -*-

from kafka import KafkaProducer
from time import sleep
import json, sys
import requests
import time
import tweepy
import sys
import apiKeys
import re
topic = 'twitter'
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
        #_producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
         _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
    
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

class CustomStreamListener(tweepy.StreamListener):
    count = 1
    def on_status(self, status):
            if(status._json['lang']=='en' and status._json['user']['location']):
                 # remove emoji
                emoji_pattern = re.compile("["
                                        u"\U0001F600-\U0001F64F"
                                        u"\U0001F300-\U0001F5FF"
                                        u"\U0001F680-\U0001F6FF"
                                        u"\U0001F1E0-\U0001F1FF"
                                        "]+", flags=re.UNICODE)
            

                try:
                    tweet = status._json['extended_tweet']['full_text']
                except:
                    tweet = status.text
                no_emoji_str = emoji_pattern.sub(r'', tweet)
                # remove url
                url_pattern = re.compile(r'(http[s]://[\w./]+)*')  
                tweet =  str(self.count)+";"+url_pattern.sub(r'', no_emoji_str).replace('\n',' ').strip()+";crash"
                prod = connect_kafka_producer();

                print(json.dumps(tweet))
                publish_message(prod, topic, tweet)

                if prod is not None:
                    prod.close()
                self.count=self.count+1
                file.write(str(tweet)+"\n")
                data.write(str(status._json) + "\n")
                sleep(1)

    def on_error(self, status_code):
        if status_code == 420:
            print(status_code)
            return False
        else:
            print(status_code)
    def on_timeout(self):
        print('Timeout...')
        return True # Don't kill the stream



if __name__== "__main__":
    if len(sys.argv) != 3:
        print("Usage:", sys.argv[0], "<location> <hash_tag>", file=sys.stderr)
        sys.exit(-1)
    if sys.argv[1] == "Boston":
        topic = "Boston4C"
    elif sys.argv[1] == "Brisbane":
        topic = "Brisbane4C"
    elif sys.argv[1] == "Chicago":
        topic = "Chicago4C"
    elif sys.argv[1] == "Dublin":
        topic = "Dublin4C"
    elif sys.argv[1] == "London":
        topic = "London4C" 
    elif sys.argv[1] == "Memphis":
        topic = "Memphis4C"
    elif sys.argv[1] == "NYC":
        topic = "NYC4C"
    elif sys.argv[1] == "SF":
        topic = "SanFrancisco4Classes"
    elif sys.argv[1] == "Seattle":
        topic = "Seattle4Classes"
    elif sys.argv[1] == "Sydney":
        topic = "Sydney4C"
    dict = {"Boston":[-71.191421, 42.227797, -70.986004, 42.399542], "Brisbane" : [152.668522848, -27.767440994, 153.31787024, -26.996844991], "Chicago" : [-87.940033, 41.644102, -87.523993, 42.0230669],
    "Dublin" : [-6.387438, 53.2987449, -6.1078047, 53.4110598], "London" : [-0.213503, 51.512805, -0.105303, 51.572068], "Memphis" : [-90.135782, 34.994192, -89.708276, 35.2728491],
    "NYC" : [144.35560512, -16.996698, 144.67020588, -16.50704904], "SF" : [-47.221994, -23.090745, -47.21694, -23.086567], "Seattle" : [-122.436232, 47.4953154, -122.2249728, 47.734319],
    "Sydney" : [150.520928608, -34.1183470085, 151.343020992, -33.578140996]}  
    consumer_key = apiKeys.consumer_key
    consumer_secret = apiKeys.consumer_secret
    access_key = apiKeys.access_key
    access_secret = apiKeys.access_secret
    geo = dict.get(sys.argv[1])
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)   
    info = api.geo_search(query = sys.argv[1])[0].bounding_box.coordinates[0]
    geo = []
    
    geo.append(info[0][0])
    geo.append(info[0][1])
    geo.append(info[2][0])
    geo.append(info[2][1])
    print("Coordinates:" )
    print(geo)
    sleep(30)
    data = open("data.json", "a+")
    file = open("tweets.txt", "a+")
   
    hash_tag = "#" + str(sys.argv[2])
    sapi = tweepy.streaming.Stream(auth, CustomStreamListener())


    sapi.filter(locations=geo, track=['accident'], is_async=True)