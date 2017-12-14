#-*- coding:utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys

es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def reinex():
    body = {
        "source": {
            "index": "dtas-201712",
            "type": "mr",
    },
        "dest": {
            "index": "dtas-201712_update",
            "type": "mr",
        }}

    es.reindex(body, wait_for_completion = False)

# 2017-01-01 00:00:00+08 2017-02-01 00:00:00+08
if __name__ == "__main__":
    reinex()