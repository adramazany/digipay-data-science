""" test_mongodb_select :
    5/16/2022 10:40 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://ops:ops%402020@10.198.110.41:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "report_mng_db"
MONGO_COLLECTION="activities"

mongoClient = MongoClient(MONGO_URI)
mongoDB = mongoClient[MONGO_DB]

query=[{"$addFields": {"minute_stamp": {"$trunc": {"$divide": ["$creationDate", 60000]}}}},
          {"$match": {"$and": [{"$and": [{"$and": [
                  {"creationDate": {"$gte": 1652659200000}}
                , {"creationDate": {"$lte": 1652679720000}}]}
                , {"creationDate": {"$gt": 0}}]}
                , {"status": {"$eq": 0}}]}},
          {"$group": {"_id": {"trunc(creationDate/60000)": {"$trunc": {"$divide": ["$creationDate", 60000]}}, "type": "$type"},
               "count(*)": {"$sum": 1}, "max(creationDate)": {"$max": "$creationDate"}, "sum(amount)": {"$sum": "$amount"}}},
          {"$project": {"minute_stamp": "$_id.trunc(creationDate/60000)", "type": "$_id.type", "minute_amount": "$sum(amount)", "minute_count": "$count(*)", "creationDate": "$max(creationDate)", "_id": 0}}]
cursor = mongoDB[MONGO_COLLECTION].aggregate(query)
df =  pd.DataFrame(list(cursor))
print(df.to_string())
