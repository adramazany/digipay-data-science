from bson import ObjectId

from digipay import mongo_to_oracle

MONGO_URI = "mongodb://ops:ops%402020@10.198.110.42:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "credit_mng_db"
MONGO_COLLECTION="purchases"
ORACLE_USER = "stage"
ORACLE_PASS = "stage"
ORACLE_DSN = "172.18.24.84/orcl"
DEST_TABLE_NAME= 'credit_purchases'
# COLUMN_TYPE="varchar2(2000)"

def clean_data(data):
    # print(data)
    if len(data)>0 and "productDelivery.products" in data:
        data["productDelivery.products"] = data["productDelivery.products"].apply(lambda x: x[:2000] )

timestamp_column_names=["creationDate","expirationDate","creditRelayDate","finalizeDate"
    ,"productDelivery.deliveryDate","productDelivery.creationDate"]

subtables=[{"col_columnname":"productDelivery.products"
                    ,"col_primarykey":"id"
                    ,"tablename":"credit_purchases_products"
                    ,"foreignkey":"f_credit_purchase"
                    ,"foreignkey_type":"number(18)"
                   }]

mongo_to_oracle.etl_incremental(MONGO_URI,MONGO_DB,MONGO_COLLECTION
                    ,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                    ,dest_column_type='varchar2(4000)'
                    ,dest_column_4max='CREATIONDATE',mongo_ordered_column='creationDate'
                    ,chunk_size=100,fn_clean_data=clean_data,fn_cnv_max_value=int
                    ,timestamp_column_names=timestamp_column_names
                    ,subtables=subtables)

