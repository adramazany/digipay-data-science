""" etl-pay_precourse_payments.py :
    4/16/2022 4:55 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

# from digipay import mongo_to_oracle
import cx_Oracle

import mongo2oracle

MONGO_URI = "mongodb://ops:ops%402020@10.198.110.41:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "pay_precourse_db"
MONGO_COLLECTION="payments"
ORACLE_USER = "mongodb" #"stage"#
ORACLE_PASS = "OraMon123" #"stage"#
ORACLE_DSN = "10.198.31.51/dgporclw"
DEST_TABLE_NAME= 'pay_precourse_payments'

def clean_data(data):
    pass
def update_max_date(oracle_dsn,oracle_user,oracle_pass):
    sql="update max_date a " \
        +" set a.g_date = trunc(sysdate -1), " \
        +" a.g_date_char = to_char (trunc(sysdate -1),'yyyymmdd'), " \
        +" a.p_date =  to_char (trunc(sysdate -1),'yyyy/mm/dd','nls_calendar=persian') " \
        +" where a.table_name in ('PAY_PRECOURSE_PAYMENTS')"

    oracleDB = cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn)
    oracle_cursor=oracleDB.cursor()
    oracle_cursor.execute(sql)
    oracleDB.commit()
    oracleDB.close()
    print("update_max_date finished.")

timestamp2jalali=["creationDate","exerciseDate","expirationDate"]
timestamp2gregorian=timestamp2jalali

subtables=[]

# init_oracle_client_path='/home/oracle/oracle_instantclient_21_1'
init_oracle_client_path='D:/app/oracle_instantclient_19_10'

mongo2oracle.etl_incremental(MONGO_URI,MONGO_DB,MONGO_COLLECTION
                             ,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                             ,dest_column_type='varchar2(4000)'
                             ,dest_column_4max='CREATIONDATE'
                             ,mongo_ordered_column='creationDate'
                             ,mongo_modification_column=None
                             ,chunk_size=1000
                             ,init_oracle_client_path=init_oracle_client_path
                             ,fn_clean_data=clean_data,fn_cnv_max_value=int
                             ,timestamp2jalali=timestamp2jalali
                             ,timestamp2gregorian=timestamp2gregorian
                             ,subtables=subtables)
# ,

#update_max_date(ORACLE_DSN,ORACLE_USER,ORACLE_PASS)

print("etl-pay_precourse_payments finished.")
