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
    query = {"creationDate": {"$gte":date.timestamp()*1000,"$lte":(date+datetime.timedelta(days=1)).timestamp()*1000}}
    options = {
        "trackingCode": 1
        ,"creationDate":1
        ,"exerciseDate":1
        ,"expirationDate":1
        ,"stateChangeDate":1
        ,"debtor.userId":1
        ,"debtor.cellNumber":1
        ,"amount":1
        ,"status":1
        ,"paymentGateway":1
        ,"creditor.firstName":1
        }

    print ("query=",query,", options=",options)
    db_rows = collection.find(query, options)
    rows=list(db_rows)
    # print(type(rows[0]))
    dateutil.fill_dates_from_timestamp(rows,"creationDate"      ,add_year_month_day=False,add_hour_min_sec=True ,add_jalali_year_month_day=True,datetime_name="datetime_createion")
    dateutil.fill_dates_from_timestamp(rows,"exerciseDate"      ,add_year_month_day=False,add_hour_min_sec=False,add_jalali_year_month_day=False,datetime_name="datetime_exercise")
    dateutil.fill_dates_from_timestamp(rows,"expirationDate"    ,add_year_month_day=False,add_hour_min_sec=False,add_jalali_year_month_day=False,datetime_name="datetime_expiration")
    dateutil.fill_dates_from_timestamp(rows,"stateChangeDate"   ,add_year_month_day=False,add_hour_min_sec=False,add_jalali_year_month_day=False,datetime_name="datetime_state_change")
    #productutil.fill_app_ipg_wallet_products(rows)
    #print("ROWS=",rows)
    count=len(rows)
    print("DATE ROWS=>",date,count)
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
ELASTIC_INDEX='donations'
#ELASTIC_BATCH_SIZE=10000
ELASTIC_BATCH_SIZE=10000

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
MONGO_COLLECTION = db.donations

start_date_jalali=jdatetime.datetime(1397,7,26)
end_date_jalali=jdatetime.datetime(1399,10,1)
total_count=0
retry_count=0

t1= int(round(time.time() * 1000))

while start_date_jalali<end_date_jalali:
    try:
        print("etl starting:",start_date_jalali)

        total_count+=etl(MONGO_COLLECTION,start_date_jalali,ELASTIC_INDEX,ELASTIC_BATCH_SIZE,UTC_TIMEZONE_GAP)

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
        MONGO_COLLECTION = db.donations

print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
client.close()
