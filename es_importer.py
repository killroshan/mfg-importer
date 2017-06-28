# pip install -U pip
# pip install psycopg2

#-*- coding:utf-8 -*-

import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')


colnames = None

conn = psycopg2.connect(database="dtaslog", user="postgres", password="Dji@123", host="127.0.0.1", port="5432")
es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def initmapping():
    global es
    mapping = {
        "properties": {
            "test_date": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ssZZ"
            },
            "details":{
                "type": "nested",
            }
        }
    }
    try:
        es.indices.create("dtas")
        es.indices.put_mapping("mr", mapping, ["dtas"])
    except:
        pass

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
    offset = 0
    id_min = long(sys.argv[1]) if len(sys.argv) >= 2 else getStartId()
    limit = 200
    print "...start at id ", id_min
    while True:
        sql = \
            "select * from (\
select * from mfg_report where id > %d order by id limit %d offset %d) as mr \
join mfg_report_detail as mrd on (mr.id = mrd.report_id)"%(id_min, limit, offset)
        batch = {} # id: {xxx, details: {xxx}}
        cursor.execute(sql)
        join_items = cursor.fetchall()
        colnames = colnames or [desc[0] for desc in cursor.description]
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
        offset += len(batch)

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
        # for mr_id, mr_doc in batch.iteritems():
        #     es.index("dtas", "mr", mr_doc, mr_id)

        # sql_mr = "select * from mfg_report where id > %d order by id limit 100 offset %s"%(id_min, offset)
        # cur_mr.execute(sql_mr)
        # mr_infos = cur_mr.fetchall()
        # offset += len(mr_infos)
        # for mr_item in mr_infos.itervalues():
        #     mr_dict = {}
        #     mr_colnames = mr_colnames or [desc[0] for desc in cur_mr.description]
        #     for idx, colname in enumerate(mr_colnames):
        #         val = mr_item[idx]
        #         if val or val == 0:
        #             if colname == "test_time":
        #                 mr_dict[colname] = str(val)
        #             elif colname == "test_date":
        #                 mr_dict[colname] = val.strftime("%Y-%m-%d %H:%M:%S%Z")
        #             else:
        #                 mr_dict[colname] = val
        #     mr_id = mr_dict["id"]
        #     sql_mrd = "select * from mfg_report_detail as mrd " \
        #               "where mrd.report_id = %d"%(mr_id,)
        #     cur_mrd.execute(sql_mrd)
        #     mrd_infos = cur_mrd.fetchall()
        #     if mrd_infos:
        #         mrd_colnames = mrd_colnames or [desc[0] for desc in cur_mrd.description]
        #         for mrd_item in mrd_infos:
        #             mrd_dict = {}
        #             for idx , colname in enumerate(mrd_colnames):
        #                 mrd_val = mrd_item[idx]
        #                 if mrd_val or mrd_val == 0:
        #                     mrd_dict[colname] = mrd_val
        #             if mrd_dict:
        #                 mr_dict.setdefault("details", []).append(mrd_dict)
        #     # print offset, mr_id
        #     # print(mr_dict["test_date"])
        #     (es.index("dtas", "mr", mr_dict, mr_id))
        #     # break
        # else:
        #     break
