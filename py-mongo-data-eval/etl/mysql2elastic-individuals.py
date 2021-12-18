from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import sys
import jdatetime
import datetime
import traceback
import logging
from digipay import dateutil
from digipay import elasticutil
from digipay import productutil
import time

#############################################################
##############      etl            ##########################
#############################################################
def etl(collection,date,target_index,batch_size,utc_timezone_gap):
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
    # ,"create_date_mongo":{"$first":{"$dateToString" : {"format":"%Y-%m-%d", "date":{"$toDate":{"$add":["$creationDate",utc_timezone_gap]}}}}}

    print ("query=",query)
    db_rows = collection.aggregate(query, allowDiskUse=True)
    rows=list(db_rows)
    # print(type(rows[0]))
    # for row in rows :
    #     row["create_date_py"]= datetime.date.fromtimestamp(row["creationDate"]/1000)
    #     row["create_date_jalali"]= str(jdatetime.date.fromtimestamp(row["creationDate"]/1000)).replace("-","")
    dateutil.fill_dates_from_timestamp(rows,"creationDate",date.timestamp()*1000)
    productutil.fill_app_ipg_wallet_products(rows)
    #print("ROWS=",rows)
    count=len(rows)
    print("DATE ROWS=>",date,count)
    # bulk_insert(rows,10000)
    elasticutil.bulk_insert_from_mongo(rows,target_index,batch_size)
    return count

#############################################################
##############      main            ##########################
#############################################################

start_time = datetime.datetime.now()
print("start-time=",datetime.datetime.now())

MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "report_mng_db"


MAX_RETRY_COUNT=3
UTC_TIMEZONE_GAP=12600000 # +03:30
# UTC_TIMEZONE_GAP=16200000 # +04:30
ELASTIC_INDEX='activities2'
#ELASTIC_BATCH_SIZE=10000
ELASTIC_BATCH_SIZE=10000

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

start_date_jalali=jdatetime.datetime(1397,11,28)
end_date_jalali=jdatetime.datetime(1399,9,21)
total_count=0
retry_count=0

t1= int(round(time.time() * 1000))

while start_date_jalali<end_date_jalali:
    try:
        print("etl starting:",start_date_jalali)

        total_count+=etl(db.activities,start_date_jalali,ELASTIC_INDEX,ELASTIC_BATCH_SIZE,UTC_TIMEZONE_GAP)

        print("etl end:",start_date_jalali," , COUNTER=",total_count," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
        start_date_jalali=(start_date_jalali+datetime.timedelta(days=1))

        #raise ValueError
    except Exception as err:
        client.close()
        print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
        logging.error(traceback.format_exc())

        print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
        print("total count till now=",total_count)

        retry_count+=1
        if retry_count>MAX_RETRY_COUNT : raise Exception("retry","retry count exceeded!")
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]

print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
client.close()
