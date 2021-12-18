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
import cx_Oracle

#############################################################
##############      etl            ##########################
#############################################################
def etl(collection,oracleCN):
    query = {}

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
MONGO_DB = "credit_mng_db"
ORACLE_USER = "test"
ORACLE_PASS = "test"
ORACLE_DSN = "172.18.24.84/orcl"

# DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 1): image not found"
cx_Oracle.init_oracle_client("/Users/adel/ds/oracle_instantclient_19_8")
oracleCN = cx_Oracle.connect(ORACLE_USER, ORACLE_PASS, ORACLE_DSN)
print("Database version:", oracleCN.version)


mongoClient = MongoClient(MONGO_URI)
mongoDB = mongoClient[MONGO_DB]

start_date_jalali=jdatetime.datetime(1397,11,28)
end_date_jalali=jdatetime.datetime(1399,9,21)
total_count=0
retry_count=0

t1= int(round(time.time() * 1000))

# while start_date_jalali<end_date_jalali:
try:
    print("etl starting:",start_date_jalali)

    total_count+=etl(mongoDB.contracts,oracleCN)

    print("etl end:",start_date_jalali," , COUNTER=",total_count," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")

except Exception as err:
    print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
    logging.error(traceback.format_exc())

    print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
    print("total count till now=",total_count)
finally:
    mongoClient.close()
    oracleCN.close()

print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
