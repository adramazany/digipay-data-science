from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import sys
import jdatetime
import datetime
import traceback
import logging
from digipay import dateutil
from digipay import elasticutil
import time

#############################################################
##############      etl            ##########################
#############################################################
def etl(collection,date,target_index,batch_size):
    query = [
        {"$match": {
            "creationDate": {"$gte":date.timestamp()*1000,"$lte":(date+datetime.timedelta(days=1)).timestamp()*1000}
        }}
        ,{"$group":{
            "_id":"$trackingCode"
            ,"amount":{"$first":"$amount"}
            ,"feeCharge":{"$first":"$feeCharge"}
        }}
        ,{"$group":{
            "_id":""
            ,"count":{"$sum":1}
            ,"amount":{"$sum":"$amount"}
            ,"feeCharge":{"$sum":"$feeCharge"}
        }}
    ]
    print ("query=",query)
    db_rows = collection.aggregate(query, allowDiskUse=True)
    rows=list(db_rows)
    # print(type(rows[0]))
    dateutil.fill_dates_from_timestamp(rows,"_id",date.timestamp()*1000 ,datetime_name="date",add_year_month_day=False,add_hour_min_sec=False)
    print("ROWS=",rows)
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


MAX_RETRY_COUNT=5
#UTC_TIMEZONE_GAP=12600000 # +03:30
UTC_TIMEZONE_GAP=16200000 # +04:30
ELASTIC_INDEX='activities_day_stat_991008'
#ELASTIC_BATCH_SIZE=10000
ELASTIC_BATCH_SIZE=31

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

start_date_jalali=jdatetime.datetime(1398,3,14)
end_date_jalali=jdatetime.datetime.now()
total_count=0
retry_count=0

while start_date_jalali<end_date_jalali:
    try:
        print("etl starting:",start_date_jalali,start_date_jalali.togregorian())
        total_count+=etl(db.activities,start_date_jalali,ELASTIC_INDEX,ELASTIC_BATCH_SIZE)
        print("etl end:",start_date_jalali," , DURATION=",(datetime.datetime.now()-start_time))
        start_date_jalali=start_date_jalali+datetime.timedelta(days=1)

        #raise ValueError
    except Exception as err:
        client.close()
        print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
        logging.error(traceback.format_exc())

        print("end-time=",datetime.datetime.now()," , DURATION=",(datetime.datetime.now()-start_time))
        print("total count till now=",total_count)

        retry_count+=1
        if retry_count>MAX_RETRY_COUNT : raise Exception("retry","retry count exceeded!")

        time.sleep(5)
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]

print("end-time=",datetime.datetime.now()," , DURATION=",(datetime.datetime.now()-start_time))
client.close()
