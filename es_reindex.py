#-*- coding:utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys

es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def reinex(start, end):
    index_name = "dtas-" + start[0:7]
    body = {"source": {
        "index": "dtas",
        "type": "mr",
        "query": {
            "range": {"test_date": {"gte": start, "lt": end}}
        }
    },
        "dest": {
            "index": index_name,
            "type": "mr",
        }}

    es.reindex(body, wait_for_completion = False)

# 2017-01-01 00:00:00+08 2017-02-01 00:00:00+08
if __name__ == "__main__":
    start = sys.argv[1]
    end   = sys.argv[2]
    print start, end
    reinex(start, end)