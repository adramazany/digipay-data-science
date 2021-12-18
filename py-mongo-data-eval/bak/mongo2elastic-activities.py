from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import sys
import jdatetime
import datetime
import traceback
import logging

#############################################################
##############      bulk-insert            ##########################
#############################################################
def clean_mongo_obj(mongo_obj):
    mongo_obj["oid"]=str(mongo_obj["_id"])
    del mongo_obj["_id"]
    return mongo_obj

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
##############      etl            ##########################
#############################################################
def etl_date(db,date):
    query = [
        {"$match": {
            "creationDate": {"$gte":date.timestamp()*1000,"$lte":(date+datetime.timedelta(days=1)).timestamp()*1000}
        }}
        ,{"$group": {
            "_id"                 : "$trackingCode"
            ,"creationDate"       :{"$first":"$creationDate"}
            ,"gateway"                :{"$first":"$gateway"}
            ,"type"                   :{"$first":"$type"}
            ,"ownerSide"              :{"$first":"$ownerSide"}
            ,"status"                 :{"$first":"$status"}
            ,"oid"                    :{"$first":"$_id.oid"}
            ,"actionCode"             :{"$first":"$actionCode"}
            ,"amount"                 :{"$first":"$amount"}
            ,"feeCharge"              :{"$first":"$feeCharge"}
            ,"dest_owner_cellNumber"  :{"$first":"$destination.owner.cellNumber"}
            ,"dest_owner_username"    :{"$first":"$destination.owner.username"}
            ,"dest_pspCode"           :{"$first":"$destination.pspCode"}
            ,"dest_identity"          :{"$first":"$destination.identity"}
            ,"dest_endpointType"      :{"$first":"$destination.endpointType"}
            ,"init_cellNumber"        :{"$first":"$initiator.cellNumber"}
            ,"init_username"          :{"$first":"$initiator.username"}
            ,"src_owner_cellNumber"   :{"$first":"$source.owner.cellNumber"}
            ,"src_owner_username"     :{"$first":"$source.owner.username"}
            ,"src_bank_code"          :{"$first":"$source.bank.cellNumber"}
            ,"src_bank_name"          :{"$first":"$source.bank.username"}
            ,"src_pspCode"            :{"$first":"$source.pspCode"}
            ,"src_identity"           :{"$first":"$source.identity"}
            ,"src_endpointType"       :{"$first":"$source.endpointType"}

            ,"owner_debtor_cellNumber"    : {"$max": {"$cond":[{"$in":["$ownerSide",[0,3]]},"$owner.cellNumber",""]} }
            ,"owner_debtor_username"      : {"$max": {"$cond":[{"$in":["$ownerSide",[0,3]]},"$owner.username",""]} }
            ,"owner_creditor_cellNumber"  : {"$max": {"$cond":[{"$in":["$ownerSide",[1,4]]},"$owner.cellNumber",""]} }
            ,"owner_creditor_username"    : {"$max": {"$cond":[{"$in":["$ownerSide",[1,4]]},"$owner.username",""]} }
            ,"owner_initiator_cellNumber" : {"$max": {"$cond":[{"$in":["$ownerSide",[2,3,4]]},"$owner.cellNumber",""]} }
            ,"owner_initiator_username"   : {"$max": {"$cond":[{"$in":["$ownerSide",[2,3,4]]},"$owner.username",""]} }
            ,"owner_mediator_cellNumber"  : {"$max": {"$cond":[{"$eq":["$ownerSide",5]},"$owner.cellNumber",""]} }
            ,"owner_mediator_username"    : {"$max": {"$cond":[{"$eq":["$ownerSide",5]},"$owner.username",""]} }
            ,"is_deptor_initiator"        : {"$max": {"$cond":[{"$eq":["$ownerSide",3]},1,""]} }
            ,"is_creditor_initiator"      : {"$max": {"$cond":[{"$eq":["$ownerSide",4]},1,""]} }
        }}
    ]
    print ("query=",query)
    db_rows = db.activities.aggregate(query, allowDiskUse=True)
    rows=list(db_rows)
    count=len(rows)
    print("DATE ROWS=>",date,count)
    bulk_insert(rows,10000)
    return count

#############################################################
##############      main            ##########################
#############################################################

start_time = datetime.datetime.now()
print("start-time=",datetime.datetime.now())

MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
#uri = "mongodb://%s:%s@%s" % (quote_plus(user), quote_plus(password), host)
MONGO_HOST = "172.16.27.11"
MONGO_PORT = 27017
MONGO_DB = "report_mng_db"
MONGO_USER = "ops"
MONGO_PASS = "ops%402020"


#client = MongoClient(MONGO_HOST, MONGO_PORT)
client = MongoClient(MONGO_URI)
# ,{  "useNewUrlParser": True,
#     "autoReconnect": True,
#     "keepAlive": 1,
#     "connectTimeoutMS": 12000000,
#     "socketTimeoutMS": 12000000,
#     "readPreference": 'secondaryPreferred'
# })
db = client[MONGO_DB]
#db.authenticate(MONGO_USER, MONGO_PASS)

#client = MongoClient(port=27017)
#db=client.test

start_date_jalali=jdatetime.datetime(1397,7,26)
end_date_jalali=jdatetime.datetime(1397,9,1)
total_count=0
try:
    while start_date_jalali<end_date_jalali:
        print("etl starting:",start_date_jalali)
        total_count+=etl_date(db,start_date_jalali)
        print("etl end:",start_date_jalali)
        start_date_jalali=start_date_jalali+datetime.timedelta(days=1)

    #raise ValueError
except Exception as err:
    print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
    logging.error(traceback.format_exc())
finally:
    client.close()
    print("end-time=",datetime.datetime.now()," , DURATION=",(datetime.datetime.now()-start_time))
    print("total count=",total_count)

