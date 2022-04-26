""" finance-import-moein-mapping-excel :
    4/24/2022 2:28 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine

import config

df = pd.read_excel("14010201-finance-moein-mapping-v1.xlsx")
print(df)


cx_Oracle.init_oracle_client(config.oracle_client_path)
engine = create_engine(config.dest_db_url)
df.to_sql("FIN_MOEIN_MAPPING",engine,if_exists='append',index=False)

print(f"importing moein mapping excel succeed with {len(df)} records.")