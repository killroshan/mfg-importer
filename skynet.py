#-*- coding:utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch import helpers


es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])


mappings = {
    "_all": {"enabled": False},
    "properties": {
        "ctime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ssZZ"
        },
        "indicator_name": {
            "type": "keyword",
        },
        "indicator_type": {
            "type": "keyword",
        },
        "indicator_value":{
            "type": "keyword",
        },
        "product":{
            "type": "keyword",
        },
        "version":{
            "type": "keyword",
        },
        "testcaseid":{
            "type": "long",
        },
        "jobdetailid":{
            "type": "long",
        },
    }
}


def makeTemplate():
    body = {
        "template": "sk-dtas-*",
        "mappings": {
            "indicator": mappings,
        }
    }
    es.indices.put_template("template_sk_dtas", body)