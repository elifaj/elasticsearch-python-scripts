from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

local_client  = Elasticsearch([{'host' : 'localhost', 'port' : 9200}])
remote_client = Elasticsearch([{'host' : 'localhost', 'port' : 9210}])

def run_stream():
     body = {
          "query" : {
               "match_all" : {}
          }
     }
     for a in scan(local_client, index = 'instagram', query = body):
          del a['_score']
          yield a

for a, b in streaming_bulk(remote_client, run_stream()):
     print(a, b)
