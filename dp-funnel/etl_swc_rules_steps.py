# from digipay import mongo_to_oracle
import cx_Oracle

import mongo2oracle

MONGO_URI = "mongodb://ops:ops%402020@10.198.110.42:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "switch_credit_db"
MONGO_COLLECTION="rules"
# ORACLE_USER = "stage"#"mongodb" #
# ORACLE_PASS = "stage"#"Mongo123" #
# ORACLE_USER = "mongodb" #"stage"#
# ORACLE_PASS = "Mongo123" #"stage"#
# ORACLE_DSN = "172.18.24.84/orcl"
ORACLE_USER = "mongodb" #"stage"#
ORACLE_PASS = "OraMon123" #"stage"#
ORACLE_DSN = "10.198.31.51/dgporclw"
DEST_TABLE_NAME= 'swc_rules'
# COLUMN_TYPE="varchar2(2000)"

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
    +" where a.table_name in ('SWC_RULES','SWC_RULES_PROFILES','SWC_RULES_STEPS','SWC_RULES_STEPS_DOCS')"

    oracleDB = cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn)
    oracle_cursor=oracleDB.cursor()
    oracle_cursor.execute(sql)
    oracleDB.commit()
    oracleDB.close()
    print("update_max_date finished.")

timestamp2jalali=["creationDate"]
timestamp2gregorian=["creationDate"]

subtables=[{"json_columnname":"profileItems"
                    ,"tablename":"swc_rules_profiles"
                    ,"auto_gen_pk_name":"id"
                    ,"master_pk":"_id"
                    ,"foreignkeys":[{"key":"_id","foreignkey":"f_rules"}]
                    ,"timestamp2jalali":[]
                    ,"timestamp2gregorian":[]
                    ,"subtables":[]
            },{"json_columnname":"steps"
               ,"tablename":"swc_rules_steps"
               ,"auto_gen_pk_name":"id"
               ,"master_pk":"_id"
               ,"foreignkeys":[{"key":"_id","foreignkey":"f_rules"}]
               ,"timestamp2jalali":[]
               ,"timestamp2gregorian":[]
               ,"subtables":[{"json_columnname":"documents"
                                 ,"tablename":"swc_rules_steps_docs"
                                 ,"auto_gen_pk_name":"id"
                                 ,"master_pk":"f_rules_steps"
                                 ,"foreignkeys":[{"key":"f_rules","foreignkey":"f_rules"}
                                                ,{"key":"code","foreignkey":"code"}]
                                 ,"timestamp2jalali":[]
                                 ,"timestamp2gregorian":[]
                              }]
             }]

mongo2oracle.etl_incremental(MONGO_URI,MONGO_DB,MONGO_COLLECTION
                    ,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                    ,dest_column_type='varchar2(4000)'
                    ,dest_column_4max='CREATIONDATE'
                    ,mongo_ordered_column='creationDate'
                    ,mongo_modification_column=None
                    ,chunk_size=500
                    ,init_oracle_client_path='d:/app/oracle_instantclient_19_10'
                    ,fn_clean_data=clean_data,fn_cnv_max_value=int
                    ,timestamp2jalali=timestamp2jalali
                    ,timestamp2gregorian=timestamp2gregorian
                    ,subtables=subtables)
# ,init_oracle_client_path='/home/oracle/oracle_instantclient_21_1'

update_max_date(ORACLE_DSN,ORACLE_USER,ORACLE_PASS)

print("etl-mongo2oracle etl_swc_rules_steps-inc finished.")
