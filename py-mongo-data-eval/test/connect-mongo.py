from pymongo import MongoClient

c = MongoClient('localhost',27017)
# c.database_names()

dbs=c.list_database_names()
print("",dbs)

db=c["test"]
cols=db.list_collection_names()
print("",cols)