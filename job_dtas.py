#-*- coding:utf-8 -*-

import pymysql
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from config import mappings, settings
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')


mappings = {
    "_all": {"enabled": False},
    "dynamic_templates":[
        {
            "nested_features":{
                "path_match": "properties.*",
                "match_mapping_type": "string",
                "mapping":{
                    "type": "keyword"
                }
            }
        }
    ],
    "properties": {
        "jobid":{
            "type": "long",
        },
        "projectid":{
            "type": "long",

        },
        "planid":{
            "type": "long",
        },
        "stime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
        },
        "ftime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
        },
        "starter":{
            "type": "keyword",
        },
        "total":{
            "type": "long",
        },
        "fin":{
            "type": "long",
        },
        "status":{
            "type": "long",
        },
        "tags":{
            "type": "keyword",
        },
        "arguments":{
            "type": "keyword",
        },
        "properties":{
            "type": "nested",
        }
    }
}

es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])



try:
    mysql = pymysql.connect(host = "localhost", user = "root", password = "Dji@123", db = "autotest")
except:
    mysql = pymysql.connect(host = "localhost", user = "autotest", password = "uix&29X@", db = "autotest")

headers = ["jobid", "projectid", "planid", "stime", "ftime", "starter", "total", "fin", "status", "tags", "arguments", "properties"]

def initmapping():
    global es
    try:
        es.indices.create("job-dtas")
        es.indices.put_mapping("job", mappings, ["job-dtas"])
    except BaseException, e:
        print str(e)

def processRow(row):
    data = {}
    for idx, header in enumerate(headers):
        if row[idx] != None:
            if header == "stime" or header == "ftime":
                data[header] = row[idx].strftime("%Y-%m-%d %H:%M:%S")
            elif header == "properties":
                try:
                    data[header] = json.loads(row[idx], encoding="utf-8")
                except:
                    print "decode json failed, %s %s"%(row[0], row[idx])
            else:
                data[header] = row[idx]
    return data

def export2ES():
    cursor = mysql.cursor()
    cursor.execute("select * from job")

    total = 0
    while True:
        rows = cursor.fetchmany(300)

        if len(rows) > 0:
            total += len(rows)
            es_batch = []
            for row in rows:
                doc = (processRow(row))
                action = {
                    "_index": "job-dtas",
                    "_type": "job",
                    "_id": doc["jobid"],
                    "_source": doc,
                }
                es_batch.append(action)
            helpers.bulk(es, es_batch)
            print "export %s docs"%(total, )
        else:
            break