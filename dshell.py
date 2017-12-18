#-*- coding:utf-8 -*-

import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from config import mappings, settings
import sys
reload(sys)
sys.setdefaultencoding('utf8')


mappings = {
    "_all": {"enabled": False},
    "dynamic_templates":[
        {
            "field_features":{
                "match_mapping_type": "string",
                "mapping":{
                    "type": "keyword"
                }
            }
        }
    ],
    "properties": {
        "id":{
            "type": "long",
        },
        "projectid":{
            "type": "long",
        },
        "ctime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
        },
        "author":{
            "type": "keyword",
        },
        "title":{
            "type": "keyword",
        },
        "device":{
            "type": "keyword",
        },
        "log":{
            "type": "text",
            "index": "no",
        }
    }
}

es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def initmapping():
    global es
    try:
        es.indices.create("dshell-dtas")
        es.indices.put_mapping("fcat", mappings, ["dshell-dtas"])
    except BaseException, e:
        print str(e)