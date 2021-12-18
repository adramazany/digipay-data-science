import mysql.connector
import sqlalchemy
import cx_Oracle
import pandas
import time
import mysql.connector



##################################################################################
def add_jalali_dates(data,timestamp_column_names):
    jalali_date_format='%Y%m%d'
    # .strftime('%Y%m%d %H:%M:%S')
    for col in timestamp_column_names :
        if col in data:
            # data['jd_'+col]=data[col]
            # # data['jd_'+col] = str(jdatetime.datetime.strftime( jdatetime.date.fromtimestamp( data[col] / 1000 ) ,  jalali_date_format ))
            data['jd_'+col] = data[col].apply(lambda x: str(jdatetime.datetime.strftime( jdatetime.date.fromtimestamp(float(x)/1000),'%Y%m%d')) if x and x.lower()!='nan' else x )
            # data['jd_'+col] = str(jdatetime.datetime.strftime( jdatetime.date.fromtimestamp(float(data[col])/1000),'%Y%m%d')) if data[col] and data[col].lower()!='nan' else data[col]

##################################################################################
def etl_append(mysqlDB,mysql_table_name,mysql_clause,oracleDB
               ,dest_table_name
               ,mysql_limit=0
               ,mysql_sort='id'
               ,dest_column_type='varchar(1000)'
               ,fn_clean_data=None
               ,timestamp_column_names=None):
    t1= time.time()
    print("etl_append: mysql_query=select * from %s where %s order by %s limit %s; dest_table_name=%s start at : %s"%(mysql_table_name,mysql_clause,mysql_sort,mysql_limit,dest_table_name,t1))


    mysql_cursor = mysqlDB.cursor()
    if mysql_limit>0:
        mysql_cursor.execute(("select * from %s where %s order by %s limit %s"%(mysql_table_name,mysql_clause,mysql_sort,mysql_limit)))
    else:
        mysql_cursor.execute(("select * from %s where %s "%(mysql_table_name,mysql_clause)))
    rows=mysql_cursor.fetchall()
    print("total readed records:%s"%(len(rows)))

    # data=pandas.json_normalize(rows)
    # data = data.applymap(lambda x: str(x).replace("'",""))
    # print("data converted to string and removed single qoutes.")
    #
    # if timestamp_column_names:
    #     add_jalali_dates(data,timestamp_column_names)

    oracle_cursor = oracleDB.cursor()
    # print("Database version:", oracleDB.version)

    # if fn_clean_data and callable(fn_clean_data) :
    #     fn_clean_data(data)
        # print("data cleaned.")

    # clean_columns = clean_column_names(data.columns)

    # fix_table_diff(oracle_cursor,dest_table_name,clean_columns,dest_column_type)

    columns = [col[0] for col in mysql_cursor.description]
    bind_names = ",".join(":" + str(i + 1) \
                          for i in range(len(rows[0])))
    insert_query = "insert into "+dest_table_name+" ("+(",".join(columns))+") values (" + bind_names + ")"

    row=[]
    try:
        counter=0
        for row in rows:
            oracle_cursor.execute(insert_query,row)
            counter+=1

        oracleDB.commit()
        print("%s records transfer completed at %s ms"%(counter , (time.time()-t1)))
        return counter
    except Exception as err:
        print("Unexpected error:", err)
        if(insert_query):print(insert_query)
        if(row):print(row)
        if (oracleDB):
            oracleDB.rollback()
            raise err

##################################################################################
def _etl_incremental(mysqlDB,mysql_table_name
                     ,oracleDB,dest_table_name,dest_column_type='varchar(1000)'
                     ,dest_column_4max='ID',mysql_ordered_column='id',chunk_size=1000
                     ,fn_clean_data=None,fn_cnv_max_value=None
                     ,timestamp_column_names=None):

    cursor=oracleDB.cursor()
    counter=1
    while counter>0 :
        query="SELECT MAX("+dest_column_4max+") FROM "+dest_table_name
        cursor.execute(query)
        result=cursor.fetchone()
        # print(result[0])
        max_value=result[0] if result[0] else 0
        if fn_cnv_max_value and callable(fn_cnv_max_value):
            max_value=fn_cnv_max_value(max_value)

        mysql_clause=" %s > %s"%(mysql_ordered_column,max_value)
        print("etl_incremental: query=%s => max_value=%s, mysql_clause=%s"%(query, max_value,mysql_clause))

        counter = etl_append(mysqlDB,mysql_table_name,mysql_clause,oracleDB,dest_table_name,chunk_size,mysql_ordered_column,dest_column_type,fn_clean_data,timestamp_column_names)

##################################################################################
def etl_from_to_jalali_date(mysqlDB,mysql_table_name,jalali_from,jalali_to
                            ,oracleDB,dest_table_name,dest_column_type='varchar2(1000)'
                            ,fn_clean_data=None):

    # mysql_query={mysql_ordered_column:{"$gte":max_value}}

    # counter = etl_append(mysqlDB,mysql_table_name,mysql_query,oracleDB,dest_table_name,dest_column_type=dest_column_type,fn_clean_data=fn_clean_data)
    pass

##################################################################################
def etl_incremental(mysql_host,mysql_port,mysql_user,mysql_password,mysql_db,mysql_table_name
                    ,oracle_dsn,oracle_user,oracle_pass
                    ,dest_table_name,dest_column_type='varchar2(1000)'
                    ,dest_column_4max='CREATIONDATE',mysql_ordered_column='creationDate',chunk_size=1000
                    ,init_oracle_client_path='/Users/adel/ds/oracle_instantclient_19_8'
                    ,fn_clean_data=None,fn_cnv_max_value=None
                    ,timestamp_column_names=None):
    t1= time.time()

    print("transfering data from mysqlDB[%s,%s,%s] to oracle[%s] starting at :%s "%(mysql_db,mysql_table_name,chunk_size,dest_table_name,time.gmtime()))

    if init_oracle_client_path :
        cx_Oracle.init_oracle_client(init_oracle_client_path)

    mysqlDB = mysql.connector.connect(
        host    =mysql_host    ,
        port    =mysql_port    ,
        user    =mysql_user    ,
        password=mysql_password
    )

    oracleDB = cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn)

    _etl_incremental(mysqlDB,mysql_db+"."+mysql_table_name,oracleDB,dest_table_name,dest_column_type,dest_column_4max,mysql_ordered_column,chunk_size,fn_clean_data,fn_cnv_max_value,timestamp_column_names)

    oracleDB.close()
    mysqlDB.close()

##################################################################################







