from pymongo import MongoClient
MONGO_URI = "mongodb://ops:ops%402020@10.198.110.41:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
# MONGO_URI = "mongodb://ops:ops%402020@10.198.110.41:27017/"
client = MongoClient(MONGO_URI)

##########################
# dbs=client.list_database_names()
# print(dbs)
##########################
# db=client["report_mng_db"]
# cols=db.list_collection_names()
# print("",cols)
##########################
# db=client["report_mng_db"]
# rows=db.activities.find({}).limit(10);
# print(list(rows))
##########################
db=client["taxi_mng_db"]
rows=db.taxi_payments.find({}).limit(10);
print(list(rows))


