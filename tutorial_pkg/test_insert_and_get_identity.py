""" test_insert_and_get_identity :
    4/25/2022 9:37 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from pprint import pprint

import cx_Oracle
import sqlalchemy
from sqlalchemy import create_engine, insert, Table, MetaData, Column, Integer, String, Identity

cx_Oracle.init_oracle_client('d:/app/oracle_instantclient_19_10')
engine = create_engine('oracle+cx_oracle://finance:finance@10.198.31.51:1521/?service_name=dgporclw', implicit_returning=True)
# sql="insert into FIN_MOEIN_REPORT_ETL (PYEAR,PMONTH) values (:pyear,:pmonth)"
# res = engine.execute(sql) # statement are not compiled
fin_moein_report_etl = Table('FIN_MOEIN_REPORT_ETL', MetaData(),
              Column('ID', Integer, Identity(start=1),primary_key=True),
              Column('PYEAR', Integer),Column('PMONTH', Integer))
# stmt = insert(fin_moein_report_etl).values(PYEAR=1399,PMONTH=1)
stmt = fin_moein_report_etl.insert().values(PYEAR=1399,PMONTH=1)
print(stmt)
# print(stmt.compile(dialect=sqlalchemy.dialects.oracle.cx_oracle.dialect()).params)
res=engine.execute(stmt)
pprint(repr(res))
print(res.inserted_primary_key[0])
