""" import-monthly-report :
    4/24/2022 3:13 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import math
import cx_Oracle
import jdatetime
import pandas as pd
import sys
from sshtunnel import SSHTunnelForwarder #Run pip install sshtunnel
from sqlalchemy import create_engine, types, insert, Table, MetaData, Column, Integer, String, Identity

oracle_client_path='d:/app/oracle_instantclient_19_10'
dest_db_url = 'oracle+cx_oracle://finance:finance@10.198.31.51:1521/?service_name=dgporclw'
sshtunnel_host="10.198.31.51"
sshtunnel_port=22
sshtunnel_user="oracle"
sshtunnel_pass="D!G!P@Y"
src_db_host="10.198.31.35"
src_db_port=1433
src_db_user="data-team"
src_db_pass="data-team"

def etl_rahkaran_monthly_moein_report(year,month):
    _src_db_host=src_db_host
    _src_db_port=src_db_port

    # start ssh tunnel when running locally
    # this paragraph should comment on the server
    sshserver = SSHTunnelForwarder(
        ssh_address_or_host=(sshtunnel_host, sshtunnel_port),remote_bind_address=(_src_db_host, _src_db_port)
        ,ssh_username=sshtunnel_user, ssh_password=sshtunnel_pass)
    sshserver.start()
    _src_db_host="127.0.0.1"
    _src_db_port=sshserver.local_bind_port

    # connect to rahkaran database
    src_engine = create_engine(url="mssql+pymssql://{user}:{password}@{host}:{port}/{db}".format(
        user=src_db_user,password=src_db_pass,
        host=_src_db_host,port=_src_db_port,db="DGPAY_3G"))

    # read moein report monthly
    start_date=jdatetime.date(year,month,1)
    end_date=jdatetime.date(year,month,jdatetime.j_days_in_month[month-1]+(1 if month==12 and start_date.isleap() else 0))
    start_date.isleap()

    sql = f"select GL.Code GLCode, SL.Code SLCode, v.Number as GNumber, v.Sequence, v.DailyNumber, v.CreationDate, v.LastModificationDate,s.Date as GDate,s.* from vwVoucherItemSummary s " \
          f" left join GL on GL.GLID=s.GLRef left join SL on SL.SLID=s.SLRef left join Voucher v on v.VoucherID=s.VoucherRef " \
          f" where s.date between parse('{start_date.strftime('%d/%m/%Y')}' as date using 'fa')" \
          f" and parse('{end_date.strftime('%d/%m/%Y')}' as date using 'fa') and s.state!=2"
    print(f"read-sql={sql}")
    df = pd.read_sql(sql,src_engine)
    print(df)

    # stop ssh tunnel when running locally
    # this paragraph should comment on the server
    sshserver.stop()

    # connect to oracle dest database
    try:
        cx_Oracle.init_oracle_client(oracle_client_path)
    except Exception as ex:
        print(ex)
    dst_engine = create_engine(url=dest_db_url)
    # deactive old masters for this yearmonth
    dst_engine.execute(f"update FIN_MOEIN_REPORT_ETL set active=0 where pyear={year} and pmonth={month} and active=1")
    # insert new master
    fin_moein_report_etl = Table('FIN_MOEIN_REPORT_ETL', MetaData(),
                                 Column('ID', Integer, Identity(start=1),primary_key=True),
                                 Column('PYEAR', Integer),Column('PMONTH', Integer))
    stmt = fin_moein_report_etl.insert().values(PYEAR=year,PMONTH=month)
    res=dst_engine.execute(stmt)
    id=res.inserted_primary_key[0]

    #clean structure and data to prepare for insertion
    df["F_MOEIN_REPORT_ETL"]=id
    columns2upper={}
    for c in df.columns:
        columns2upper[c]=c.upper()
    df.rename(columns=columns2upper,inplace=True)
    df.fillna("null",inplace=True)

    # insert details
    for i,row in df.iterrows():
        try:
            sql_ins = "insert into FIN_MOEIN_REPORT_DETAIL (" \
                      "F_MOEIN_REPORT_ETL,GLCODE,SLCODE,GNUMBER,SEQUENCE,DAILYNUMBER" \
                      ",CREATIONDATE,LASTMODIFICATIONDATE,GDATE,VOUCHERITEMID,VOUCHERREF,BRANCHREF,GLREF,SLREF,DEBIT,CREDIT" \
                      ",TARGETCURRENCYREF,TARGETCURRENCYDEBIT,TARGETCURRENCYCREDIT,FISCALYEARREF" \
                      ",VOUCHERTYPEREF,STATE,LEDGERREF,DLLEVEL4,DLLEVEL5,DLLEVEL6,DLLEVEL7,DLLEVEL8,DLLEVEL9" \
                      ")values(" \
                      f"{row['F_MOEIN_REPORT_ETL']},{row['GLCODE']},{row['SLCODE']},{row['GNUMBER']},{row['SEQUENCE']},{row['DAILYNUMBER']}" \
                      f",to_date(substr('{row['CREATIONDATE']}',1,19),'YYYY-MM-DD HH24:MI:SS'),to_date(substr('{row['LASTMODIFICATIONDATE']}',1,19),'YYYY-MM-DD HH24:MI:SS')" \
                      f",to_date('{row['GDATE']}','YYYY-MM-DD HH24:MI:SS'),{row['VOUCHERITEMID']},{row['VOUCHERREF']}" \
                      f",{row['BRANCHREF']},{row['GLREF']},{row['SLREF']},{row['DEBIT']},{row['CREDIT']}" \
                      f",{row['TARGETCURRENCYREF']},{row['TARGETCURRENCYDEBIT']},{row['TARGETCURRENCYCREDIT']}" \
                      f",{row['FISCALYEARREF']},{row['VOUCHERTYPEREF']},{row['STATE']},{row['LEDGERREF']}" \
                      f",{row['DLLEVEL4']},{row['DLLEVEL5']},{row['DLLEVEL6']},{row['DLLEVEL7']},{row['DLLEVEL8']},{row['DLLEVEL9']})"
            dst_engine.execute(sql_ins)

            if int(i%100)==0:
                print(i)
        except Exception as ex:
            print(ex,sql_ins)

    # update header aggregations
    sql_update_agg="update (select etl.ID,etl.DETAIL_COUNT,etl.DETAIL_Credit_SUM,etl.DETAIL_Debit_SUM" \
        ",detail.F_MOEIN_REPORT_ETL,detail.DETAIL_COUNT2" \
        ",detail.DETAIL_Debit_SUM2,detail.DETAIL_Credit_SUM2" \
        " from FIN_MOEIN_REPORT_ETL etl" \
        " join (select F_MOEIN_REPORT_ETL,count(*) DETAIL_COUNT2" \
        ",sum(Debit) DETAIL_Debit_SUM2,sum(Credit) DETAIL_Credit_SUM2" \
        " from FIN_MOEIN_REPORT_DETAIL group by F_MOEIN_REPORT_ETL) detail" \
        " on detail.F_MOEIN_REPORT_ETL=etl.ID" \
        f" where etl.id={id})" \
        " set" \
        " DETAIL_COUNT =DETAIL_COUNT2," \
        " DETAIL_Debit_SUM =DETAIL_Debit_SUM2," \
        " DETAIL_Credit_SUM =DETAIL_Credit_SUM2" \
        f" where id={id}"
    print(f"sql_update_agg={sql_update_agg}")
    dst_engine.execute(sql_update_agg)

def etl_rahkaran_basetables():
    _src_db_host=src_db_host
    _src_db_port=src_db_port

    # start ssh tunnel when running locally
    # this paragraph should comment on the server
    sshserver = SSHTunnelForwarder(
        ssh_address_or_host=(sshtunnel_host, sshtunnel_port),remote_bind_address=(src_db_host, src_db_port)
        ,ssh_username=sshtunnel_user, ssh_password=sshtunnel_pass)
    sshserver.start()

    _src_db_host="127.0.0.1"
    _src_db_port=sshserver.local_bind_port

    src_engine = create_engine(url="mssql+pymssql://{user}:{password}@{host}:{port}/{db}".format(
        user=src_db_user,password=src_db_pass,
        host=_src_db_host,port=_src_db_port,db="DGPAY_3G"))

    try:
        cx_Oracle.init_oracle_client(oracle_client_path)
    except Exception as ex:
        print(ex)
    dst_engine = create_engine(url=dest_db_url)

    basetables= {"FIN_GL":"select * from GL",
                 "FIN_SL":"select * from SL",
                 "FIN_DL":"select * from DL",
                 "FIN_ACCOUNT":"select * from Account",
                 "FIN_ACCOUNTGROUP":"select * from AccountGroup",
                 "FIN_DLTYPE":"select * from DLType",
                 "FIN_VOUCHERTYPE":"select * from VoucherType"
                }

    for key in basetables:
        sql = basetables[key]
        print(f"read-sql={sql}")
        df = pd.read_sql(sql,src_engine)

        columns2upper={}
        for c in df.columns:
            columns2upper[c]=c.upper()
        df.rename(columns=columns2upper,inplace=True)
        df.rename(columns={"DATE":"GDATE"},inplace=True)
        df.fillna("null",inplace=True)
        df.replace({False: 0, True: 1}, inplace=True)

        # check all columns datatype match with data
        # df.applymap(type).nunique()
        df_nunique=df.applymap(type).nunique()
        # print(df_nunique)
        df_nunique=df_nunique[df_nunique>1]
        print(df_nunique)
        for c in df_nunique.index:
            # df[c].astype(float)
            # print(df[c].to_string())
            # del df[c]
            df[c]='??'
            # df[c].loc[~df[c].isnull()]='???'

        print(df[:5].to_string())

        # print(df.dtypes)
        change_object_clob_columns_varchar={col:types.VARCHAR(df[col].str.len().max()*2 if not math.isnan(df[col].str.len().max()) else 1) for col,val in df.dtypes.items() if val=="object" }
        print(change_object_clob_columns_varchar)
        # write to database
        try:
            dst_engine.execute(f"drop table {key}")
        except Exception as ex:
            print(ex)
        df.to_sql(key,dst_engine,if_exists='replace',index=False
                  , dtype=change_object_clob_columns_varchar)

    sshserver.stop()


if __name__ == "__main__":
    if len(sys.argv)!=2:
        print("syntax:python3 import-monthly-report.py 140001")
        sys.exit()
    yearmonth = int(sys.argv[1])
    etl_rahkaran_monthly_moein_report(int(yearmonth/100),int(yearmonth%100))

    # for i in range(1,13):
    #     etl_rahkaran_monthly_moein_report(1397,i)
    # for i in range(1,13):
    #     etl_rahkaran_monthly_moein_report(1398,i)
    # for i in range(1,13):
    #     etl_rahkaran_monthly_moein_report(1399,i)

    # etl_rahkaran_monthly_moein_report(1401,1)
    # etl_rahkaran_basetables()