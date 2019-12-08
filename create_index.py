from elasticsearch import Elasticsearch
import json
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
request_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}

es.indices.create(index='tweeter_accidents', body=request_body)


import json
es.index(index='tweeter_accidents', doc_type='_doc',
                 id=1,
                 body={
                         "location" : "NYC",
                         "content" : "NO"})
#
# from elasticsearch import Elasticsearch
# import json
#
# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
