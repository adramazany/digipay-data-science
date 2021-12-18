from bson import ObjectId

from digipay import mongo_to_oracle

MONGO_QUERY_all={}
MONGO_QUERY_id= {"_id" : ObjectId('5f5e0246ebeb6105689ccc19')}


# MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27019/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_URI = "mongodb://ops:ops%402020@10.198.110.43:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "kyc_db"
MONGO_QUERY= MONGO_QUERY_all
MONGO_COLLECTION="credit_inquiries"
ORACLE_USER = "stage"
ORACLE_PASS = "stage"
ORACLE_DSN = "172.18.24.84/orcl"
DEST_TABLE_NAME= 'kyc_credit_inquiries'
# COLUMN_TYPE="varchar2(2000)"

# def clean_data(data):
#     if len(data)>0:
#         data["steps"] = data["steps"].apply(lambda x: x[:2000-len(x)] if len(x)>=2000 else x )

# mongo_to_oracle.etl(MONGO_URI,MONGO_DB,MONGO_COLLECTION,MONGO_QUERY,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
#                     ,mode='drop',mongo_limit=0,dest_column_type='varchar2(4000)'
#                     ,chunk_size=1000)
mongo_to_oracle.etl_incremental(MONGO_URI,MONGO_DB,MONGO_COLLECTION
                                ,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                                ,dest_column_type='varchar2(4000)'
                                ,dest_column_4max='CREATIONDATE',mongo_ordered_column='creationDate'
                                ,chunk_size=10000,fn_clean_data=None,fn_cnv_max_value=int
                                ,timestamp_column_names=None)

