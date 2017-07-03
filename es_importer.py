#-*- coding:utf-8 -*-

# pip install -U pip
# pip install psycopg2


import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')


colnames = None

conn = psycopg2.connect(database="dtaslog", user="postgres", password="Dji@123", host="127.0.0.1", port="5432")
es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def initmapping():
    global es
    mappings = {
        "_all": {"enabled": False},
        "properties": {
            "test_date": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ssZZ"
            },
            "station_id": {
                "type": "string",
                "index": "not_analyzed",
            },
            "station_name": {
                "type": "string",
                "index": "not_analyzed",
            },
            "dut_sn":{
                "type": "string",
                "analyzer": "tri_analyzer",
            },
            "test_time":{
                "type": "string",
                "index": "not_analyzed",
            },
            "test_site":{
                "type": "string",
                "index": "not_analyzed"
            },
            "op_id":{
                "type": "string",
                "index": "not_analyzed",
            },
            "part_number":{
                "type": "string",
                "index": "not_analyzed",
            },
            "test_sw_rev":{
                "type": "string",
                "index": "not_analyzed",
            },
            "pc_name": {
                "type": "string",
                "index": "not_analyzed",
            },
            # test_status为Pass, PASS, PAss...等，所以用默认的standard tokenizer转换为小写，查询时也要用小写的pass或者fail
            # "test_status":{
            #     "index": "standard"
            # },
            "log":{
                "type": "text",
                "index": "no"
            },
            "details":{
                "type": "nested",
                "properties":{
                    "description": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "item_name":{
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "unit":{
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "detail":{
                        "type": "text",
                        "index": "no",
                    }
                }
            }
        }
    }
    settings = {
        "settings":{
            "analysis": {
                "analyzer": {
                    "tri_analyzer": {
                        "tokenizer": "tri_tokenizer"
                    }
                },
                "tokenizer": {
                    "tri_tokenizer": {
                        "type": "ngram",
                        "min_gram": 3,
                        "max_gram": 14,
                    }
                }
            }
        }
    }
    try:
        es.indices.create("dtas", settings)
        es.indices.put_mapping("mr", mappings, ["dtas"])
    except BaseException,e:
        print str(e)

def getStartId():
    body = {
        "sort": {"id": "desc"},
        "size": 1,
        "stored_fields": []
    }
    try:
        ret = es.search("dtas", "mr", body)
        return long(ret["hits"]["hits"][0]["_id"])
    except:
        return -1

if __name__ == "__main__":
    initmapping()
    cursor = conn.cursor()
    id_min = long(sys.argv[1]) if len(sys.argv) >= 2 else getStartId()
    limit = 250
    print "...start at id ", id_min
    while True:
        sql = \
            "select * from (\
select * from mfg_report where id > %d order by id limit %d) as mr \
join mfg_report_detail as mrd on (mr.id = mrd.report_id)"%(id_min, limit)
        batch = {} # id: {xxx, details: {xxx}}
        cursor.execute(sql)
        join_items = cursor.fetchall()
        colnames = colnames or [desc[0] for desc in cursor.description]
        if len(join_items) == 0:
            break
        for val in join_items:
            report_id = val[0]
            report_info = batch.setdefault(report_id, {})
            if not report_info: # padding common field
                for col_no, common_colname in enumerate(colnames[0: 14]):
                    if common_colname == "test_date":
                        report_info[common_colname] = val[col_no].strftime("%Y-%m-%d %H:%M:%S%Z")
                    elif common_colname == "test_time":
                        report_info[common_colname] = str(val[col_no])
                    else:
                        report_info[common_colname] = val[col_no]
            # padding detail fields
            detail = {}
            for col_no, detail_col_name in enumerate(colnames[14:]):
                col_no += 14
                detail[detail_col_name] = val[col_no]
            report_info.setdefault("details", []).append(detail)
        actions = []
        for key, val in batch.iteritems():
            action ={
                "_index": "dtas",
                "_type": "mr",
                "_id": key,
                "_source": val,
            }
            actions.append(action)
        helpers.bulk(es, actions)
        id_min = join_items[-1][0]
        sys.stdout.write("...import date to %s, last_id = %s \r"%(join_items[-1][9], id_min))
        sys.stdout.flush()
        time.sleep(0.01)