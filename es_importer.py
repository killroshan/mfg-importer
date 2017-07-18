#-*- coding:utf-8 -*-

# pip install -U pip
# pip install psycopg2


import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from config import mappings, settings
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')


colnames = None

try:
    conn = psycopg2.connect(database="dtaslog", user="postgres", password="Dji@123", host="127.0.0.1")
except:
    conn = psycopg2.connect(database="dtaslog", user="autotest", password="woxI*&12", host="10.17.1.14")

# try:
#     es = Elasticsearch(hosts=[{"host": "10.17.1.14", "port": 9200}])
# except:
es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def initmapping():
    global es
    try:
        es.indices.create("dtas", settings)
        es.indices.put_mapping("mr", mappings, ["dtas"])
    except BaseException,e:
        print str(e)

def makeTemplate():
    body = {
        "template": "dtas-*",
        "settings": settings["settings"],
        "mappings": {
            "_default_": mappings,
        }
    }
    print body
    es.indices.put_template("template_dtas", body)

def getStartTime():
    body = {
        "sort": {"test_date": "desc"},
        "size": 1,
        "_source": ["test_date"]
    }
    try:
        ret = es.search("dtas-*", "mr", body)
        return ret[u"hits"][u"hits"][0][u"_source"]["test_date"]
    except:
        return "1970-01-01 00:00:00+08"

def getTotal():
    try:
        return es.count("dtas-*", "mr")[u"count"]
    except:
        return 0

last_msg = ""
def noscrollp(msg):
    print msg
    # global last_msg
    # delta = len(msg) - len(last_msg)
    # if delta >= 0:
    #     sys.stdout.write(msg + "\r")
    # else:
    #     sys.stdout.write("".join([msg, " " * abs(delta), "\r"]))
    # sys.stdout.flush()
    # last_msg = msg

def checkStandAlong():
    import os
    try:
        os.mkdir(os.path.join(os.getcwd(), "es-lock"))
    except:
        print "lock file exist"
        sys.exit(0)

def clearLock():
    import os
    try:
        os.rmdir(os.path.join(os.getcwd(), "es-lock"))
    except:
        pass

if __name__ == "__main__":
    checkStandAlong()
    try:
        cursor = conn.cursor()
        start_time = sys.argv[1] if len(sys.argv) >= 2 else getStartTime()
        end_time = sys.argv[2] if len(sys.argv) >= 3 else datetime.datetime.fromtimestamp(time.time() - 24 * 3600).strftime("%Y-%m-%d %H:%M:%S%Z")

        limit = 300
        total = 0
        offset = 0
        print "start at %s, end at %s"%(start_time, end_time)
        while True:
            sql = \
                "select * from (\
select * from mfg_report where test_date >= '%s' and test_date <= '%s' order by test_date limit %d offset %d) as mr \
left join mfg_report_detail as mrd on (mr.id = mrd.report_id)"%(start_time, end_time, limit, offset)
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
                        if common_colname == "test_time":
                            report_info[common_colname] = str(val[col_no])
                        else:
                            report_info[common_colname] = val[col_no]
                # padding detail fields
                detail = {}
                if val[14] is not None: # details is not none
                    for col_no, detail_col_name in enumerate(colnames[14:]):
                        col_no += 14
                        detail[detail_col_name] = val[col_no]
                    detail_item_name = detail["item_name"]
                    report_info.setdefault("details", []).append(detail)
                    report_info.setdefault("item_names", []).append(detail_item_name)

            sorted_reports = sorted(batch.itervalues(), lambda a, b: cmp(a["test_date"], b["test_date"]))
            es_batch = []
            es_batch_size = 0
            es_batch_max_size = 3000
            latest_datetime = sorted_reports[-1]["test_date"]
            for report_info in sorted_reports:
                report_info["test_date"] = report_info["test_date"].strftime("%Y-%m-%d %H:%M:%S%Z")  # convert to string after sort
                index_suffix = report_info["test_date"][0:7]
                details_len = len(report_info["details"]) if report_info.has_key("details") else 0
                if details_len >= 100:
                    continue
                report_id = report_info["id"]
                report_info["item_names"] = "||||".join(set(report_info.setdefault("item_names", [])))
                if es_batch_size + details_len > es_batch_max_size:
                    if es_batch:
                        total += len(es_batch)
                        noscrollp("import batch size %s to ES, total %s"%(len(es_batch), total))
                        helpers.bulk(es, es_batch)
                        es_batch = []
                        es_batch_size = 0

                        es_batch_size += details_len
                        action = {
                            "_index": "dtas-" + index_suffix,
                            "_type": "mr",
                            "_id": report_id,
                            "_source": report_info,
                        }
                        es_batch.append(action)
                else:
                    es_batch_size += details_len
                    action = {
                        "_index": "dtas-" + index_suffix,
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
            _offset = 1
            for report in sorted_reports[-2::-1]:
                if report["test_date"] == latest_time:
                   _offset += 1
                else:
                    break

            if latest_time == start_time:
                offset += _offset
                if offset >= 5000:
                    offset = 0
                    latest_time = (latest_datetime + datetime.timedelta(seconds = 1)).strftime("%Y-%m-%d %H:%M:%S%Z")
                    # f = open("log", "a+")
                    # f.write(start_time + "\n")
                    # f.close()
            else:
                offset = _offset
            start_time = latest_time
            time.sleep(0.01)
    except BaseException ,e:
        print e
        clearLock()
        sys.exit(-1)
    clearLock()
    sys.exit(0)