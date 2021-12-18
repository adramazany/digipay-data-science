from pymongo import MongoClient

MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256"
#uri = "mongodb://%s:%s@%s" % (quote_plus(user), quote_plus(password), host)
MONGO_HOST = "172.16.27.11"
MONGO_PORT = 27017
MONGO_DB = "report_mng_db"
MONGO_USER = "ops"
MONGO_PASS = "ops%402020"

#client = MongoClient(MONGO_HOST, MONGO_PORT)
client = MongoClient(MONGO_URI)
#db = client[MONGO_DB]
#db.authenticate(MONGO_USER, MONGO_PASS)

dbs=client.list_database_names()
print(dbs)

db=client["report_mng_db"]
cols=db.list_collection_names()
print("",cols)