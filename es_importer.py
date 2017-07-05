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

def getStartTime():
    body = {
        "sort": {"test_date": "desc"},
        "size": 1,
        "_source": ["test_date"]
    }
    try:
        ret = es.search("dtas", "mr", body)
        return ret[u"hits"][u"hits"][0][u"_source"]["test_date"]
    except:
        return "1970-01-01 00:00:00+08"

def getTotal():
    try:
        return es.count("dtas", "mr")[u"count"]
    except:
        return 0

last_msg = ""
def noscrollp(msg):
    global last_msg
    delta = len(msg) - len(last_msg)
    if delta >= 0:
        sys.stdout.write(msg + "\r")
    else:
        sys.stdout.write("".join([msg, " " * abs(delta), "\r"]))
    sys.stdout.flush()
    last_msg = msg

if __name__ == "__main__":
    initmapping()
    cursor = conn.cursor()
    start_time = long(sys.argv[1]) if len(sys.argv) >= 2 else getStartTime()
    operator = ">="
    limit = 200
    total = 0
    offset = 0
    print "start at ", start_time
    while True:
        sql = \
            "select * from (\
select * from mfg_report where test_date %s '%s' order by test_date asc limit %d offset %d) as mr \
join mfg_report_detail as mrd on (mr.id = mrd.report_id)"%(operator, start_time, limit, offset)
        batch = {} # id: {xxx, details: {xxx}}
        noscrollp("reading from db, limit = %s, offset = %s, start_time = %s"%(limit, offset, start_time))
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
                    # if common_colname == "test_date":
                    #     pass
                        # report_info[common_colname] = val[col_no].strftime("%Y-%m-%d %H:%M:%S%Z")
                    if common_colname == "test_time":
                        report_info[common_colname] = str(val[col_no])
                    else:
                        report_info[common_colname] = val[col_no]
            # padding detail fields
            detail = {}
            for col_no, detail_col_name in enumerate(colnames[14:]):
                col_no += 14
                detail[detail_col_name] = val[col_no]
            report_info.setdefault("details", []).append(detail)

        sorted_reports = sorted(batch.itervalues(), lambda a, b: cmp(a["test_date"], b["test_date"]))
        es_batch = []
        es_batch_size = 0
        es_batch_max_size = 3000
        for report_info in sorted_reports:
            report_id = report_info["id"]
            report_info["test_date"] = report_info["test_date"].strftime("%Y-%m-%d %H:%M:%S%Z") # convert to string after sort
            details_len = len(report_info["details"])
            if details_len >= 100:
                continue
            if es_batch_size + details_len > es_batch_max_size:
                if es_batch:
                    total += len(es_batch)
                    noscrollp("import batch size %s to ES, total %s"%(len(es_batch), total))
                    helpers.bulk(es, es_batch)
                    es_batch = []
                    es_batch_size = 0

                    es_batch_size += details_len
                    action = {
                        "_index": "dtas",
                        "_type": "mr",
                        "_id": report_id,
                        "_source": report_info,
                    }
                    es_batch.append(action)
            else:
                es_batch_size += details_len
                action = {
                    "_index": "dtas",
                    "_type": "mr",
                    "_id": report_id,
                    "_source": report_info,
                }
                es_batch.append(action)
        else:
            if es_batch:
                total += len(es_batch)
                noscrollp("import batch size %s to ES, total %s" % (len(es_batch), total))
                helpers.bulk(es, es_batch)
                es_batch = []
                es_batch_size = 0

        latest_time = sorted_reports[-1]["test_date"] #string
        if latest_time == start_time:
            offset += len(sorted_reports)
            operator = ">="
        else:
            start_time = latest_time
            operator = ">"
            offset = 0
        time.sleep(0.01)