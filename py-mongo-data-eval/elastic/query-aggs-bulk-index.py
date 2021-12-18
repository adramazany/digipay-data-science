import datetime
import jdatetime
from elasticsearch import Elasticsearch,helpers
import requests
import pprint

#############################################################
##############      bulk-insert            ##########################
#############################################################

def bulk_insert(acts,batch_size):
    es = Elasticsearch()
    for i in range(0, len(acts), batch_size):
        batch = acts[i : min(i + batch_size, len(acts))]
        actions = [
            {
                '_index': 'activities',
                '_id'   : '%s'%(batch[j]["_id"]),
                '_source': clean_mongo_obj(batch[j])
            }
            for j in range(len(batch))
        ]
        helpers.bulk(es, actions)


#############################################################
##############      main            ##########################
#############################################################

query={"query":{
    "aggs": {
        "total_by_date_year": {
            "terms": {
                "field": "date_time"
            },
            "aggs": {
                "total_count": {"sum": {"field": "count"}}
                ,"total_amount": {"sum": {"field": "amount"}}
                ,"total_feeCharge": {"sum": {"field": "feeCharge"}}
            }
        }
    }
}}



counter=0
es = Elasticsearch()

result = helpers.scan(es, query=query,index="activities_test")
for doc in result:
    pprint.pprint(doc)
    # es.update(index=document['_index'], doc_type=document['_type'], id=document['_id'], body={
    #     "script" : {
    #         "source": "return [ _doc : ctx.payload.aggregations.the_foos.buckets ]"
    #     }
    # })
