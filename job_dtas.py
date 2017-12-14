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
                    jdict = json.loads(row[idx], encoding="utf-8")
                    if jdict.has_key("retry_list"):
                        if len(jdict["retry_list"]) == 0:
                            del jdict["retry_list"]
                            # print jdict["retry_list"], row[0], "retry"

                    # if jdict.has_key("merge_list"):
                    #     if len(jdict["merge_list"]) == 0:
                    #         print jdict["merge_list"], row[0], "merge"
                    #
                    # if jdict.has_key("retry_jobids"):
                    #     if len(jdict["retry_jobids"]) == 0:
                    #         print jdict["retry_jobids"], row[0], "retry_jobids"
                    data[header] = jdict
                except:
                    pass
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