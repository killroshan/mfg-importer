#-*- coding:utf-8 -*-

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
        "log":{
            "type": "text",
            "index": "no"
        },
        "item_names":{
            "type": "text",
            "analyzer": "item_names_analyzer",
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
                    "index": "not_analyzed",
                },
                "usl":{
                    "type": "double",
                    "index": "no",
                },
                "lsl":{
                    "type": "double",
                    "index": "no",
                },
                "result":{
                    "type": "double",
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
                },
                "item_names_analyzer":{
                    "tokenizer": "item_tokenizer",
                }
            },
            "tokenizer": {
                "tri_tokenizer": {
                    "type": "ngram",
                    "min_gram": 3,
                    "max_gram": 14,
                },
                "item_tokenizer":{
                    "type": "pattern",
                    "pattern": "\|\|\|\|",

                }
            }
        }
    }
}
