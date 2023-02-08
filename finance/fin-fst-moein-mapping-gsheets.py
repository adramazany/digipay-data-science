import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine

gsheet_url = "https://docs.google.com/spreadsheets/d/1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1-8/export?format=csv&gid=0"
oracle_client_path='d:/app/oracle_instantclient_19_10'
db_url = 'oracle+cx_oracle://finance:finance@10.198.31.51:1521/?service_name=dgporclw'

df_gsheet =  pd.read_csv(gsheet_url,header=0)
print('df_gsheet',df_gsheet.to_string())

# connect to oracle dest database
try:
    cx_Oracle.init_oracle_client(oracle_client_path)
except Exception as ex:
    print(ex)
db_engine = create_engine(url=db_url)
sql= "select fst_02_code,moein,dllevel5 from FIN_FST_MOEIN"
df_exists = pd.read_sql(sql,db_engine)
columns2upper={}
for c in df_exists.columns:
    columns2upper[c]=c.upper()
df_exists.rename(columns=columns2upper,inplace=True)
print('df_exists',df_exists.to_string())


sql_history="insert into fin_fst_moein_history " \
            " select sysdate as modified_date,fm.fst_02_code,fm.moein,fm.dllevel5 from FIN_FST_MOEIN fm"
db_engine.execute(sql_history)

df_common = df_exists.merge(df_gsheet, on=['FST_02_CODE','MOEIN','DLLEVEL5'],copy=True)
print('common:', df_common.to_string())

df_del=df_exists[(~df_exists['MOEIN'].isin(df_common['MOEIN'])) | (~df_exists['FST_02_CODE'].isin(df_common['FST_02_CODE']))]
del_query = "delete from FIN_FST_MOEIN where MOEIN=:moein and FST_02_CODE=:fst_02_code"
for i,row in df_del.iterrows():
    db_engine.execute(del_query,moein=int(row['MOEIN']),fst_02_code=int(row['FST_02_CODE']))
print('deleted:',df_del.to_string())

df_add=df_gsheet[(~df_gsheet['MOEIN'].isin(df_common['MOEIN'])) | (~df_gsheet['FST_02_CODE'].isin(df_common['FST_02_CODE']))]
ins_query = "insert into FIN_FST_MOEIN (MOEIN,FST_02_CODE,DLLEVEL5) values (:moein,:fst_02_code,:dllevel5)"
for i,row in df_add.iterrows():
    db_engine.execute(ins_query,moein=row['MOEIN'],fst_02_code=row['FST_02_CODE'],dllevel5=row['DLLEVEL5'])
print('inserted:',df_add.to_string())

