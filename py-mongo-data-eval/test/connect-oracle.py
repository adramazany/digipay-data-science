import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
# db_url = 'oracle+cx_oracle://mongodb:Mongo123@10.198.31.51:1521/?service_name=dgporclw'
db_url = 'oracle+cx_oracle://mongodb:Mongo123@172.18.24.84:1521/?service_name=orcl'
engine = create_engine(db_url)
df = pd.read_sql("select * from bank", engine)
print(df)
