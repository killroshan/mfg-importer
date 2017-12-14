import pymysql
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

helpers = [""]

def initmapping():
    global es
    try:
        es.indices.create("job-dtas")
        es.indices.put_mapping("job", mappings, ["job-dtas"])
    except BaseException, e:
        print str(e)

def export2ES():
    cursor = mysql.cursor()
    cursor.execute("select * from job")

    while True:
        rows = cursor.fetchmany(1000)
        if len(rows) > 0:
            print(rows[0])
            return

        break

    # cursor1 = mysql.cursor()
    # print(cursor1.execute("select count(*) from job"))
