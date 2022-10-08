import cx_Oracle
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

data_1 = {'product': ['computer','monitor','printer','desk','just_in_1',301],
          'price': [1200,800,200,350,10,511114]
          }
df1 = pd.DataFrame(data_1)

oracle_client_path='d:/app/oracle_instantclient_19_10'
db_url = 'oracle+cx_oracle://finance:finance@10.198.31.51:1521/?service_name=dgporclw'

try:
    cx_Oracle.init_oracle_client(oracle_client_path)
except Exception as ex:
    print(ex)
db_engine = create_engine(url=db_url)

print(df1.to_string())

sql="insert into tmp_1 (product,price)values(:product,:price)"
for i,row in df1.iterrows():
    print(row['product'],row['price'])
    db_engine.execute(sql,product=row['product'],price=row['price'])

print("finished.")