from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import sys
import jdatetime
import datetime
import traceback
import logging
from digipay import dateutil
from digipay import elasticutil

#############################################################
##############      etl            ##########################
#############################################################
def etl(collection,month,target_index,batch_size,utc_timezone_gap):
    query = [
        {"$match": {
            "creationDate": {"$gte":month.timestamp()*1000,"$lte":(dateutil.add_month_jalali(month)).timestamp()*1000}
        }}
        ,{"$group":{
            "_id":"$trackingCode"
            ,"createDate":{"$first":{"$dateToString" : {"format":"%Y-%m-%d", "date":{"$toDate":{"$add":["$creationDate",utc_timezone_gap]}}}}}
            ,"amount":{"$first":"$amount"}
            ,"feeCharge":{"$first":"$feeCharge"}
        }}
        ,{"$group":{
            "_id":"$createDate"
            ,"count":{"$sum":1}
            ,"amount":{"$sum":"$amount"}
            ,"feeCharge":{"$sum":"$feeCharge"}
        }}
    ]
    print ("query=",query)
    db_rows = collection.aggregate(query, allowDiskUse=True)
    rows=list(db_rows)
    # print(type(rows[0]))
    for row in rows : row["jdate"]= str(jdatetime.date.fromtimestamp(datetime.datetime.strptime(row["_id"],"%Y-%m-%d").timestamp())).replace("-","")
    print("ROWS=",rows)
    count=len(rows)
    print("DATE ROWS=>",month,count)
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
#UTC_TIMEZONE_GAP=12600000 # +03:30
UTC_TIMEZONE_GAP=16200000 # +04:30
ELASTIC_INDEX='activities_stat'
#ELASTIC_BATCH_SIZE=10000
ELASTIC_BATCH_SIZE=31

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

start_date_jalali=jdatetime.datetime(1398,1,1)
end_date_jalali=jdatetime.datetime(1399,10,1)
total_count=0
retry_count=0

while start_date_jalali<end_date_jalali:
    try:
        print("etl starting:",start_date_jalali)
        total_count+=etl(db.activities,start_date_jalali,ELASTIC_INDEX,ELASTIC_BATCH_SIZE,UTC_TIMEZONE_GAP)
        print("etl end:",start_date_jalali," , DURATION=",(datetime.datetime.now()-start_time))
        start_date_jalali=dateutil.add_month_jalali(start_date_jalali)

        #raise ValueError
    except Exception as err:
        client.close()
        print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
        logging.error(traceback.format_exc())

        print("end-time=",datetime.datetime.now()," , DURATION=",(datetime.datetime.now()-start_time))
        print("total count till now=",total_count)

        retry_count+=1
        if retry_count>MAX_RETRY_COUNT : raise Exception("retry","retry count exceeded!")
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]

print("end-time=",datetime.datetime.now()," , DURATION=",(datetime.datetime.now()-start_time))
client.close()
