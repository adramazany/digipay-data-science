def clean_mongo_obj(mongo_obj):
    mongo_obj["oid"]=str(mongo_obj["_id"])
    del mongo_obj["_id"]
    return mongo_obj

