import datetime
import jdatetime
from elasticsearch import Elasticsearch,helpers
from digipay import dateutil
import time
import pprint
import requests

#############################################################
##############      query            ##########################
#############################################################

query_match_all={
    "query": {
        "match_all": {}
    }
    #,"fields":["creationDate"]
}
query_empty_jalali_date={
    "query": {
        "bool":{"must_not":[
            {"exists":{"field":"jalali_date"}}
        ]}}
    #,"fields":["creationDate"]
}

#############################################################
##############      bulk update            ##################
#############################################################

def bulk_update(es,rows,index_name):
    actions = [
        {   '_op_type': 'update'
            ,'_index': index_name
            ,'_id'   : row["_id"]
            ,'_source': {'doc':row['_source']}
            # ,"script" : { update just required fields is #60% slower than update document
            #     "source": "ctx._source.datetime=params.datetime;"
            #               "ctx._source.year=params.year;"
            #               "ctx._source.month=params.month;"
            #               "ctx._source.day=params.day;"
            #               "ctx._source.hour=params.hour;"
            #               "ctx._source.minute=params.minute;"
            #               "ctx._source.second=params.second;"
            #               "ctx._source.jalali_date=params.jalali_date;"
            #               "ctx._source.jalali_year=params.jalali_year;"
            #               "ctx._source.jalali_month=params.jalali_month;"
            #               "ctx._source.jalali_day=params.jalali_day;"
            #               ,"lang": "painless"
            #               ,"params" : row['_source']
            #     }
        }
        for row in rows
    ]
    helpers.bulk(es, actions)

#############################################################
##############      main            ##########################
#############################################################
ES_INDEX_NAME="activities"
# ES_QUERY=query_match_all
ES_QUERY=query_empty_jalali_date
BATCH_SIZE=10000
counter=0
es = Elasticsearch()
rows=list()

resp = requests.post('http://localhost:9200/'+ES_INDEX_NAME+'/_count',json=ES_QUERY)
if resp.status_code != 200:
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())


t1= int(round(time.time() * 1000))
print("start bulk dates update of  ",ES_INDEX_NAME)
for doc in helpers.scan(es, query=ES_QUERY,index=ES_INDEX_NAME):
    try:
        rows.append(doc)
        counter+=1
        if counter%BATCH_SIZE==0 :
            print("counter:",counter,", duration=",(int(round(time.time() * 1000))-t1),"(ms)")
            dateutil.fill_dates_from_timestamp(rows,"creationDate",data_in_sub_field_source="_source")
            bulk_update(es,rows,ES_INDEX_NAME)
            rows.clear()
    except Exception as err:
        pprint.pprint(err)
        pprint.pprint(doc)
        raise err


if len(rows)>0 :
    print("counter:",counter)
    dateutil.fill_dates_from_timestamp(rows,"creationDate",data_in_sub_field_source="_source")
    bulk_update(es,rows,ES_INDEX_NAME)
    rows.clear()

print("update succeed. counter=",counter,", duration=",(int(round(time.time() * 1000))-t1),"(ms)")

