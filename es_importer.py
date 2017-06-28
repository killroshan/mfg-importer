# pip install -U pip
# pip install psycopg2

#-*- coding:utf-8 -*-

import psycopg2
from elasticsearch import Elasticsearch
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')


mr_colnames = None
mrd_colnames = None

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
    es.indices.create("dtas")
    es.indices.put_mapping("mr", mapping, ["dtas"])


if __name__ == "__main__":
    print sys.argv
    cur_mr = conn.cursor()
    cur_mrd = conn.cursor()
    offset = 0
    while True:
        sql_mr = "select * from mfg_report as mr " \
              "order by mr.id " \
              "limit 1 offset %s"%(offset, )
        cur_mr.execute(sql_mr)
        mr_infos = cur_mr.fetchall()
        if mr_infos:
            offset += 1
            mr_dict = {}
            mr_item = mr_infos[0]
            mr_colnames = mr_colnames or [desc[0] for desc in cur_mr.description]
            for idx, colname in enumerate(mr_colnames):
                val = mr_item[idx]
                if val or val == 0:
                    if colname == "test_time":
                        mr_dict[colname] = str(val)
                    elif colname == "test_date":
                        mr_dict[colname] = val.strftime("%Y-%m-%d %H:%M:%S%Z")
                    else:
                        mr_dict[colname] = val
            mr_id = mr_dict["id"]
            sql_mrd = "select * from mfg_report_detail as mrd " \
                      "where mrd.report_id = %d"%(mr_id,)
            cur_mrd.execute(sql_mrd)
            mrd_infos = cur_mrd.fetchall()
            if mrd_infos:
                mrd_colnames = mrd_colnames or [desc[0] for desc in cur_mrd.description]
                for mrd_item in mrd_infos:
                    mrd_dict = {}
                    for idx , colname in enumerate(mrd_colnames):
                        mrd_val = mrd_item[idx]
                        if mrd_val or mrd_val == 0:
                            mrd_dict[colname] = mrd_val
                    if mrd_dict:
                        mr_dict.setdefault("details", []).append(mrd_dict)
            # print offset, mr_id
            # print(mr_dict["test_date"])
            print(es.index("dtas", "mr", mr_dict, mr_id))
            # break
        else:
            break
