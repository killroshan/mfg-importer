import pymongo
import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pymongo import ReadPreference
from elasticsearch.exceptions import NotFoundError

import sys

try:
    mc = pymongo.MongoClient("10.17.3.21", 27017)
except:
    mc = pymongo.MongoClient("localhost", 27017)

try:
    es = Elasticsearch(hosts=[{"host": "10.17.1.11", "port": 9200},
                              {"host": "10.17.1.12", "port": 9200},
                              {"host": "10.17.1.13", "port": 9200},
                              {"host": "10.17.1.14", "port": 9200}])
except:
    es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

def process_mongo_doc(doc):
    item_names_list = []
    if len(doc.get("details", [])) >= 500:
        return None

    for test_item in doc.get("details", []):
        if test_item.has_key("item_name"):
            item_names_list.append(test_item["item_name"])

    # process item_names
    item_names = "||||".join(item_names_list)
    if item_names:
        doc["item_names"] = item_names

    # process test_date
    if doc.has_key("test_date") and doc.has_key("_id"):
        doc["id"] = doc["_id"]
        doc["test_date"] = doc["test_date"].strftime("%Y-%m-%d %H:%M:%S%Z") + "+08"
        del doc["_id"]
        return doc
    else:
        return None

def find_last_id(index):
    body = {
        "sort": {"id": "desc"},
        "size": 1,
        "_source": ["id"]
    }
    try:
        ret = es.search(index, "mr", body)
        return ret[u"hits"][u"hits"][0][u"_source"]["id"]
    except NotFoundError:
        return 0


if __name__ == "__main__":
    suffix = sys.argv[1] # 201701~201708
    index = "dtas-" + suffix
    last_id = find_last_id(index)

    db = mc.get_database("dtas-npi-db", read_preference=ReadPreference.SECONDARY)
    co = db.get_collection("npi-report-" + suffix)

    it = co.find({"_id": {"$gt": last_id}}, batch_size = 500, sort = [("_id", 1)])

    total = it.count()
    print "begin at %s, total doc num: %d "%(last_id, total)
    count = 0
    batch = []

    try:
        while it:
            doc = process_mongo_doc(it.next())
            count += 1
            if doc:
                doc_id = doc["id"]
                action = {
                    "_index": "dtas-" + suffix,
                    "_type": "mr",
                    "_id": doc_id,
                    "_source": doc,
                }
                batch.append(action)
                if len(batch) >= 100:
                    print "import batch size %s, last_id = %s, percent = %.2f"%(len(batch), doc_id, count / float(total))
                    helpers.bulk(es, batch)
                    batch = []
                    # time.sleep(0.01)

    except StopIteration:
        if batch:
            print "import last batch size %s, last_id = %s" % (len(batch), doc_id)
            helpers.bulk(es, batch)
            batch = []