#-*- coding:utf-8 -*-

import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from config import mappings, settings
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')

try:
    conn = psycopg2.connect(database="dtaslog", user="postgres", password="Dji@123", host="127.0.0.1")
except:
    conn = psycopg2.connect(database="dtaslog", user="autotest", password="woxI*&12", host="10.17.1.14")

# try:
#     es = Elasticsearch(hosts=[{"host": "10.17.1.14", "port": 9200}])
# except:
es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])


if __name__ == "__main__":
    sn = sys.argv[1]
    cursor = conn.cursor()
    sql = "select * from mfg_report_0 as mr \
left join mfg_report_detail_0 as mrd on (mr.id = mrd.report_id) \
where mr.dut_sn = '%s'"%(sn, )
    cursor.execute(sql)
    join_items = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    batch = {}

    for val in join_items:
        report_id = val[0]
        report_info = batch.setdefault(report_id, {})
        if not report_info:  # padding common field
            for col_no, common_colname in enumerate(colnames[0: 14]):
                if common_colname == "test_time":
                    report_info[common_colname] = str(val[col_no])
                else:
                    report_info[common_colname] = val[col_no]
        # padding detail fields
        detail = {}
        if val[14] is not None:  # details is not none
            for col_no, detail_col_name in enumerate(colnames[14:]):
                col_no += 14
                detail[detail_col_name] = val[col_no]
            detail_item_name = detail["item_name"]
            report_info.setdefault("details", []).append(detail)
            report_info.setdefault("item_names", []).append(detail_item_name)

    reports = batch.values()

    es_batch = []
    for report in reports:
        report["test_date"] = report["test_date"].strftime("%Y-%m-%d %H:%M:%S%Z")  # convert to string after sort
        index_suffix = report["test_date"][0:7]
        report["item_names"] = "||||".join(set(report.setdefault("item_names", [])))
        report_id = report["id"]


        es_batch.append({
            "_index": "dtas-" + index_suffix + "-plus",
            "_type": "mr",
            "_id": report_id,
            "_source": report,
        })

    # for _batch in es_batch:
    #     print _batch
    #     print ">>>>>>>>>>>>>>>>"


    if es_batch:
        helpers.bulk(es, es_batch)
        print "import success"