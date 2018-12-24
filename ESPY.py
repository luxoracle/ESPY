
# coding: utf-8
# power by LukeLiu

# ubuntu 18.04
# python 3.6.6
# elasticsearch 6.5.0

from elasticsearch import Elasticsearch
import pandas as pd


## 索引操作

# 删除索引
def index_delete(es_server, es_index):
    es_server.indices.delete(index=es_index, ignore=404)
    return 0

#创建索引
def index_create(es_server, es_index):
    es_server.indices.create(index=es_index, ignore=400)
    return 0


## 文件（数据）写入

# 批量写入数据
def doc_insert(es_server, es_index, es_type, es_doc):
    es_server.bulk(index=es_index,doc_type=es_type,body=es_doc)
    return 0

# 替换某id的数据
def doc_replace(es_server, es_index, es_type, es_id, es_doc):
    es_server.index(index=es_index, doc_type=es_type, body=es_doc, id=es_id)
    return 0


## 文件（数据）读取

# 读取全部数据
# size最大10000，可修改
def get_all(es_server, es_index, es_type, start=0, size=10):
    body = {
            "query":{
                      "match_all":{}
                    },
            "from":start,
            "size":size,
            }

    dic = es_server.search(index=es_index, doc_type=es_type, body=body)
    print("total count: " + str(dic['hits']['total']))    
    count = min(dic['hits']['total']-start, size)
    
    slc = dic['hits']['hits'][0]
    slc.update(slc['_source'])
    slc.pop('_source')
    df = pd.DataFrame(slc, index=[start])
    
    for i in range(1,int(count)):
        slc = dic['hits']['hits'][i]
        slc.update(slc['_source'])
        slc.pop('_source')
        dfp = pd.DataFrame(slc, index=[start+i])
        df = df.append(dfp)
    return df

# 按关键词读取
# size最大10000，可修改
def get_value(es_server, es_index, es_type, es_keyword, es_value, es_bool="must", start=0, size=10):
    body = {
            "query":{
                    "bool":{
                            es_bool: [{"term": { es_keyword+".keyword" : es_value }}]
                            }
                    },
            "from": start,
            "size": size,
            "sort": [ ],
            "aggs": { }
            }
    
    dic = es_server.search(index=es_index, doc_type=es_type, body=body)    
    print("match count: " + str(dic['hits']['total']))    
    count = min(dic['hits']['total']-start, size)
    
    slc = dic['hits']['hits'][0]
    slc.update(slc['_source'])
    slc.pop('_source')
    df = pd.DataFrame(slc, index=[start])
    
    for i in range(1,int(count)):
        slc = dic['hits']['hits'][i]
        slc.update(slc['_source'])
        slc.pop('_source')
        dfp = pd.DataFrame(slc, index=[start+i])
        df = df.append(dfp)
    return df

