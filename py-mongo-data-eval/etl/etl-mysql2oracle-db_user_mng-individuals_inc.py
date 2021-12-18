from bson import ObjectId

from digipay import mysql_to_oracle

MYSQL_DSN='mysql+mysqlconnector://ops:ops%402020@10.198.110.63:3306/db_user_mng'
MYSQL_HOST="10.198.110.63"
MYSQL_PORT="3306"
MYSQL_USER="ops"
MYSQL_PASSWORD="ops@2020"
MYSQL_DB = "db_user_mng"
MYSQL_TABLE_NAME="individuals"
ORACLE_USER = "stage"
ORACLE_PASS = "stage"
ORACLE_DSN = "172.18.24.84/orcl"
DEST_TABLE_NAME= 'individuals'
# COLUMN_TYPE="varchar2(2000)"

# def clean_data(data):
#     # print(data)
#     if len(data)>0 and "productDelivery.products" in data:
#         data["productDelivery.products"] = data["productDelivery.products"].apply(lambda x: x[:2000] )
#
# timestamp_column_names=["creationDate","expirationDate","creditRelayDate","finalizeDate"
#     ,"productDelivery.deliveryDate","productDelivery.creationDate"]
mysql_to_oracle.etl_incremental(MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWORD,MYSQL_DB,MYSQL_TABLE_NAME
                    ,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                    ,dest_column_type='varchar2(4000)'
                    ,dest_column_4max='ID',mysql_ordered_column='id'
                    ,chunk_size=10000,fn_clean_data=None,fn_cnv_max_value=int
                    ,timestamp_column_names=None)
