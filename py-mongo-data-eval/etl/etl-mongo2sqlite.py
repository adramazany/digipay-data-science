import datetime
import time
import sqlite3
from os.path import expanduser
from pymongo import MongoClient
import pandas


query_match_all = {}


MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27018/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "credit_mng_db"
MONGO_QUERY=query_match_all
TABLE_NAME= 'credit_contracts'
DB_URL= expanduser("~")+'/ds/datafile/'+TABLE_NAME+'.db'
# COLUMN_TYPE="text"
COLUMN_TYPE="varchar(1000)"

t1= int(round(time.time() * 1000))

mongoClient = MongoClient(MONGO_URI)
mongoDB = mongoClient[MONGO_DB]

db_rows = mongoDB.contracts.find(MONGO_QUERY).limit(1)
rows=list(db_rows)
print(rows)

data=pandas.json_normalize(rows)
data = data.applymap(lambda x: str(x))
# data["_id"] = data["_id"].apply(lambda x: str(x))

# print(data,type(data),data.columns)

try:
    create_query = "create table if not exists {0} ({1} )".format(TABLE_NAME, (" "+COLUMN_TYPE+",").join(data.columns).replace(".","_").replace("_id","id").replace("_class","class")+" "+COLUMN_TYPE)
    print(create_query)
    delete_query = "delete from "+TABLE_NAME
    insert_query = "insert into {0} ({1}) values (?{2})".format(TABLE_NAME, ",".join(data.columns).replace(".","_").replace("_id","id").replace("_class","class"), ",?" * (len(data.columns)-1))
    print(insert_query)

    print("insert has started at " + str(datetime.datetime.now()))
    db = sqlite3.connect(DB_URL)
    print("Successfully Connected to SQLite")
    c = db.cursor()
    c.execute(create_query)
    c.execute(delete_query)
    print(data.values.tolist())
    c.executemany(insert_query , data.values.tolist())
    # values.clear()
    db.commit()
    c.close()
    print("insert has completed at " + str(datetime.datetime.now()))

except Exception as error:
    print("Failed to insert data into sqlite table", error)
finally:
    if (db):
        db.close()
        print("The SQLite connection is closed")
