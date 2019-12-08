#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 17 17:20:28 2019

@author: arthur
"""

from elasticsearch import Elasticsearch
import hashlib
import json

es = Elasticsearch([{'host':'localhost', 'port':9200}])
request_body = {
        "settings":{
                "number_of_shards": 1,
                "number_of_replicas": 0
        }
}
        
es.indices.create(index = 'tweeter_accidents', body = request_body)

es.index(index='tweeter_accidents', doc_type='_doc', 
                 id=hashlib.sha256(json.dumps("").encode('ascii','ignore')).hexdigest(), 
                 body={
                         "location" : "NYC", 
                         "predicted" : 0})