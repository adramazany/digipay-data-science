""" etl-auto-activities-10minute :
    5/15/2022 5:01 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import cx_Oracle
import jdatetime
import datetime
import pandas as pd
import pytz
from sqlalchemy import create_engine
from pymongo import MongoClient

MONGO_URI = "mongodb://ops:ops%402020@10.198.110.41:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "report_mng_db"
MONGO_COLLECTION="activities"
oracle_client_path='d:/app/oracle_instantclient_19_10'
dest_db_url = 'oracle+cx_oracle://cdc:CdC123@10.198.31.51:1521/?service_name=dgporclw'
activity_type_to_ftype = {
    0:10,    1:11,    2:12,    3:13,    4:14,    5:15,    10:16,    11:17,
    12:18,    13:19,    14:20,    15:21,    16:22,    17:23,    30:24,    31:25,
    32:26,    40:27,    50:28,    60:29,    70:30,    80:31,    90:32,    91:33,
    92:34,    93:35,    94:36,    100:37,    110:38,    111:39,    112:40,    113:41,
    120:42,    130:43,    140:44,    150:45,    160:46,    170:47,    33:431,
    190:4571,    200:5492,
}

print(f"{datetime.datetime.now()} \t starting...")

cx_Oracle.init_oracle_client(oracle_client_path)
dst_engine = create_engine(url=dest_db_url)

max_exists_creationdate = dst_engine.execute("select max(creationdate) from CDC_ACTIVITIES_10MIN_AGG_OK").scalar()
max_exists_creationdate = max_exists_creationdate if max_exists_creationdate else 0

now = datetime.datetime.now()
# today_start_timestamp_utc = datetime.datetime(now.year,now.month,now.day).timestamp()*1000
today_start_timestamp_utc = datetime.datetime(2022,3,21).timestamp()*1000
# now_prev_minute_utc = int((datetime.datetime(now.year,now.month,now.day,now.hour,now.minute) - datetime.timedelta(minutes=1)).timestamp()*1000)
now_prev_minute_utc = int((datetime.datetime(now.year,now.month,now.day,now.hour,now.minute)).timestamp()*1000)-1
start_timestamp_utc= int(max(today_start_timestamp_utc,max_exists_creationdate))

print(f"{datetime.datetime.now()} \t get ranges: max_exists_creationdate={max_exists_creationdate}, today_start_timestamp_utc={today_start_timestamp_utc}, now_prev_minute_utc={now_prev_minute_utc}, start_timestamp_utc={start_timestamp_utc}")

mongoClient = MongoClient(MONGO_URI)
mongoDB = mongoClient[MONGO_DB]

# sql = f"select trunc(creationDate/60000) as minute_stamp,type" \
#       f",sum(amount) minute_amount" \
#       f",count(*) minute_count" \
#       f",max(creationDate) creationDate" \
#       f" from activities a" \
#       f" where creationDate between 1652659200000 and 1652679720000" \
#       f" and creationDate>0" \
#       f" and a.status=0" \
#       f" group by trunc(creationDate/60000),type"
query=[{"$addFields": {"minute_stamp": {"$trunc": {"$divide": ["$creationDate", 60000]}}}},
       {"$match": {"$and": [{"$and": [
           {"creationDate": {"$gte": start_timestamp_utc }}
           , {"creationDate": {"$lte": now_prev_minute_utc }}]}
           , {"status": {"$eq": 0}}]}},
       {"$group": {"_id": {"trunc(creationDate/60000)": {"$trunc": {"$divide": ["$creationDate", 60000]}}, "type": "$type"},
                   "count(*)": {"$sum": 1}, "max(creationDate)": {"$max": "$creationDate"}, "sum(amount)": {"$sum": "$amount"}}},
       {"$project": {"minute_stamp": "$_id.trunc(creationDate/60000)", "type": "$_id.type", "minute_amount": "$sum(amount)", "minute_count": "$count(*)", "creationDate": "$max(creationDate)", "_id": 0}}]
print(query)
cursor = mongoDB[MONGO_COLLECTION].aggregate(query, allowDiskUse=True)
df =  pd.DataFrame(list(cursor))
print(f"{datetime.datetime.now()} \t read from mongo.activities count={len(df)}")

# minute_stamp  type  minute_amount  minute_count   creationDate
# to_upper columns
columns2upper={}
for c in df.columns:
    columns2upper[c]=c.upper()
df.rename(columns=columns2upper,inplace=True)

# prepare other columns
timezone=pytz.timezone('Asia/Tehran')
df["GDATE"] = df["CREATIONDATE"].apply(lambda x: datetime.datetime.fromtimestamp(float(x)/1000).astimezone(pytz.utc) )
df["PDATE"] = df["CREATIONDATE"].apply(lambda x: int(jdatetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%Y%m%d')) )
df["HOUR_MINUTE"] = df["CREATIONDATE"].apply(lambda x: int(jdatetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%H%M')))
df["F_TYPE"] = df["TYPE"].apply(lambda x:activity_type_to_ftype.get(x,-1))
del df["MINUTE_STAMP"]

print(f"{datetime.datetime.now()} \t columns and data prepared")

# print(df.to_string())
print(df)

df.to_sql("CDC_ACTIVITIES_10MIN_AGG_OK",dst_engine,if_exists='append',index=False)

print(f"{datetime.datetime.now()} \t finished.")
