from elasticsearch import Elasticsearch, helpers
from digipay import mongoutil
import datetime
import jdatetime
from digipay import config


def bulk_insert_from_mongo(objs,index_name,batch_size):
    es = Elasticsearch(http_auth=(config.ES_USER,config.ES_PASS))
    for i in range(0, len(objs), batch_size):
        batch = objs[i : min(i + batch_size, len(objs))]
        actions = [
            {
                '_index': index_name
                ,'_id'   : '%s'%(batch[j]["_id"])
                ,'_source': mongoutil.clean_mongo_obj(batch[j])
            }
            for j in range(len(batch))
        ]
        helpers.bulk(es, actions)
