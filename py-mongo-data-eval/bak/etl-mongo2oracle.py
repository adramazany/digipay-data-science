import datetime
import time
from os.path import expanduser
from pymongo import MongoClient
import pandas
import cx_Oracle


query_match_all = {}


MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27018/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "credit_mng_db"
MONGO_QUERY=query_match_all
TABLE_NAME= 'credit_contracts'
# COLUMN_TYPE="text"
COLUMN_TYPE="varchar(2000)"
ORACLE_USER = "stage"
ORACLE_PASS = "stage"
ORACLE_DSN = "172.18.24.84/orcl"

# DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 1): image not found"
cx_Oracle.init_oracle_client("/Users/adel/ds/oracle_instantclient_19_8")

t1= int(round(time.time() * 1000))

mongoClient = MongoClient(MONGO_URI)
mongoDB = mongoClient[MONGO_DB]

# db_rows = mongoDB.contracts.find(MONGO_QUERY).limit(1)
db_rows = mongoDB.contracts.find(MONGO_QUERY)
rows=list(db_rows)
print(rows)

data=pandas.json_normalize(rows)
# data = data.applymap(lambda x: str(x).replace("\"[","").replace("\"]",""))
data = data.applymap(lambda x: str(x).replace("'","").replace("'",""))
# data["_id"] = data["_id"].apply(lambda x: str(x))

# print(data,type(data),data.columns)

try:

    create_query = "create table  {0} ({1} )".format(TABLE_NAME, (" "+COLUMN_TYPE+",").join(data.columns).replace(".","_").replace("_id","id").replace("_class","class")+" "+COLUMN_TYPE)
    print(create_query)
    # delete_query = "delete from "+TABLE_NAME
    delete_query = "drop table "+TABLE_NAME
    # insert_query = "insert into {0} ({1}) values (?{2})".format(TABLE_NAME, ",".join(data.columns).replace(".","_").replace("_id","id").replace("_class","class"), ",?" * (len(data.columns)-1))
    bind_names = ",".join(":" + str(i + 1) \
                          for i in range(len(data.columns)))
    insert_query = "insert into "+TABLE_NAME+" values (" + bind_names + ")"
    print(insert_query)

    print("insert has started at " + str(datetime.datetime.now()))
    db = cx_Oracle.connect(ORACLE_USER, ORACLE_PASS, ORACLE_DSN)
    print("Database version:", db.version)
    print("Successfully Connected to SQLite")
    c = db.cursor()
    c.execute(delete_query)
    c.execute(create_query)
    # print(data.values.tolist())
    for row in data.values.tolist():
        print(row)
        c.execute(insert_query , row)
    # values.clear()
    db.commit()
    c.close()
    print("insert has completed at " + str(datetime.datetime.now()))

# except Exception as error:
#     print("Failed to insert data into database table", error)
finally:
    if (db):
        db.close()
        print("The connection is closed")
