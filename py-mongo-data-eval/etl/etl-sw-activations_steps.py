# from digipay import mongo_to_oracle
import cx_Oracle

import mongo2oracle

MONGO_URI = "mongodb://ops:ops%402020@10.198.110.42:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "switch_credit_db"
MONGO_COLLECTION="activations"
ORACLE_USER = "mongodb"
ORACLE_PASS = "Mongo123"
# ORACLE_USER = "stage"
# ORACLE_PASS = "stage"
ORACLE_DSN = "172.18.24.84/orcl"
DEST_TABLE_NAME= 'sw_credit_activations'
# COLUMN_TYPE="varchar2(2000)"

def int_float(x):
    return int(float(x))
def clean_data(data):
    # print(data)
    # if len(data)>0 and "steps" in data:
    #     data["productDelivery.products"] = data["productDelivery.products"].apply(lambda x: x[:2000] )
    pass
def update_max_date(oracle_dsn,oracle_user,oracle_pass):
    sql="update max_date a " \
    +" set a.g_date = trunc(sysdate -1), " \
    +" a.g_date_char = to_char (trunc(sysdate -1),'yyyymmdd'), " \
    +" a.p_date =  to_char (trunc(sysdate -1),'yyyy/mm/dd','nls_calendar=persian') " \
    +" where a.table_name in ('SW_CREDIT_ACTIVATIONS','SW_CREDIT_ACTIVATION_STEPS','SW_CREDIT_ACTIVATION_STEPS_DOCS')"

    oracleDB = cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn)
    oracle_cursor=oracleDB.cursor()
    oracle_cursor.execute(sql)
    oracleDB.commit()
    oracleDB.close()
    print("update_max_date finished.")

timestamp2jalali=["completedDate","creationDate","dueDate"]
timestamp2gregorian=["completedDate","creationDate","dueDate"]

subtables=[{"json_columnname":"steps"
                    ,"tablename":"sw_credit_activation_steps"
                    ,"auto_gen_pk_name":"id"
                    ,"master_pk":"_id"
                    ,"delete_column":"f_credit_activations"
                    ,"foreignkeys":[{"key":"_id","foreignkey":"f_credit_activations"}
                                    ,{"key":"userId","foreignkey":"userid"}]
                    ,"timestamp2jalali":["startedDate","completedDate"]
                    ,"timestamp2gregorian":["startedDate","completedDate"]
                    ,"subtables":[{"json_columnname":"documents"
                                      ,"tablename":"sw_credit_activation_steps_docs"
                                      ,"auto_gen_pk_name":"id"
                                      ,"master_pk":"f_credit_activations_steps"
                                      ,"delete_column":"f_credit_activations"
                                      ,"foreignkeys":[{"key":"f_credit_activations","foreignkey":"f_credit_activations"}
                                                      ,{"key":"userid","foreignkey":"userid"}]
                                      ,"timestamp2jalali":["uploadDate","opsActionDate"]
                                      ,"timestamp2gregorian":["uploadDate","opsActionDate"]
                                   }]
            }]


# سلام فیلدهایی که cdc کالکشن activation روی اونا باید انجام بشه
# starteddate و completeddate تو steps
# و uploaddate و opsactiondate تو docs هستن


mongo2oracle.etl_incremental(MONGO_URI,MONGO_DB,MONGO_COLLECTION
                    ,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                    ,dest_column_type='varchar2(4000)'
                    ,dest_column_4max='CREATIONDATE'
                    ,mongo_ordered_column='creationDate'
                    ,mongo_modification_column='statusModificationDate'
                    ,chunk_size=100
                    ,init_oracle_client_path='d:/app/oracle_instantclient_19_10'
                    ,fn_clean_data=clean_data,fn_cnv_max_value=int_float
                    ,timestamp2jalali=timestamp2jalali
                    ,timestamp2gregorian=timestamp2gregorian
                    ,subtables=subtables)
# ,init_oracle_client_path='/home/oracle/oracle_instantclient_21_1'

update_max_date(ORACLE_DSN,ORACLE_USER,ORACLE_PASS)

print("etl-mongo2oracle-switch_credit_db-activations_steps-inc finished.")
