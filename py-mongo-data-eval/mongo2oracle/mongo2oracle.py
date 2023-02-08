# mongo2oracle etl tools is (c) 2021 Adel Ramezani <adramazany at gmail.com>.
# The mongo2oracle etl tools module was contributed to Python as of Python 3.8 and thus
# was licensed under the Python license. Same license applies to all files in
# the mongo2oracle package project.
import logging
import re
import time
import datetime
import pytz
import yaml
from pymongo import MongoClient
import pandas
import cx_Oracle
import jdatetime
import numpy as np
from sys import getsizeof

MIN_SIZE_VARCHAR=20
MIN_SIZE_NUMBER=12

##################################################################################
insert_query=""
update_query=""
subtable_insert_query=""
##################################################################################
logging.basicConfig(level=logging.INFO ,format='%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
##################################################################################
def str_abbreviation_first_word_1(st,limit,sep='_',start_pos=0):
    if len(st)>limit:
        sep_first_pos=st.find(sep,start_pos)+1
        st=(st[0:start_pos+1]+"_"+st[sep_first_pos:])
        start_pos=start_pos+2
        if start_pos>limit:
            return st[:limit]
        return str_abbreviation_first_word(st,limit,sep,start_pos)
    return st
def str_abbreviation_first_word(st,limit,sep='_',start_pos=0):
    while len(st)>limit:
        sep_first_pos=st.find(sep,start_pos)+1
        st=(st[0:start_pos+1]+"_"+st[sep_first_pos:])
        start_pos=start_pos+2
        if start_pos>limit:
            return st[:limit]
    return st
##################################################################################
def clean_column_name(col,cleans_columns):
    #column name is number
    if myisnumeric(col):
        col='N_%s'%(col)
    #remove non english chars
    col_en = col.encode("ascii", "ignore").decode().replace(" ","_").replace("\\","_") \
        .replace(".","_").replace(")","_").replace("(","_") \
        .replace("_id","id").replace("_class","class")
    if len(str(col_en))==0:
        col='U_%s'%(len(col))
    else:
        col=col_en
    #first char is digit
    if col[0].isdigit() or col[0]=='_':
        col='N_%s'%(col)
    #limit of column name is 31
    if len(col)>31:
        col = str_abbreviation_first_word(col,31)
    # return col.upper()
    clean_name = col.upper().replace("OPTION","OPTION_").replace("ORDER","ORDER_")

    clean_name = check_clean_name_exist_iter_name(clean_name,cleans_columns)

    return clean_name
##################################################################################
def clean_column_names(columns):
    cleans_columns=list()
    for col in columns :
        cleans_columns.append(clean_column_name(col,columns))
    return cleans_columns

##################################################################################
def mylen(obj):
    if isinstance(obj,str):
        return len(obj)
    elif isinstance(obj,int):
        return 12
    elif isinstance(obj,float):
        return 18
    else:
        return len(str(obj))
##################################################################################
def myisnumeric(obj):
    if isinstance(obj,int) or isinstance(obj,float) or isinstance(obj,complex):
        return True
    else:
        return False
##################################################################################
def check_clean_name_exist_iter_name(clean_name, cleans_columns):
    if clean_name in cleans_columns and clean_name[29:]:
        print(">>>> clean_name is already exists!!! : {}".format(clean_name))
        counter=0
        for x in cleans_columns:
            if x[:29]==clean_name[:29] and x[29:] and x[29:].isnumeric():
                counter=max(counter,int(x[29:]))
        counter += 1
        max_clean_name="%s%02d"%(clean_name[:29],counter)
        return max_clean_name
    else:
        return clean_name

def clean_column_name_length_isnumeric(df):
    cleans_columns=dict()

    # print('df.dtypes=',df.dtypes)
    if len(df)>0 :
        # measurer = np.vectorize(len)
        measurer = np.vectorize(mylen)
        df_columns_length = dict(zip(df, measurer(df.values).max(axis=0)))
        # df_apply_str = df.apply(str)
        # df_columns_length = dict(zip(df_apply_str, measurer(df_apply_str.values).max(axis=0)))

        # df_columns_isnumeric=df.apply(lambda s: pandas.to_numeric(s, errors='coerce').notnull().all())
        df_columns_numeric_medians = df.agg(['median','max'])
        # print('df_columns_numeric_medians=',df_columns_numeric_medians)
        # df_columns_isnumeric=dict()
        # for col in df.columns : df_columns_isnumeric[col]=(col in df_columns_numeric_medians)
        # print('df_columns_isnumeric=',df_columns_isnumeric)

        for col in df.columns :
            val={'original':col
                ,'length':(df_columns_length[col] if df_columns_length[col]<=4000 else 4000)
                ,'isnumeric':(col in df_columns_numeric_medians and 'median' in df_columns_numeric_medians[col] and  df_columns_numeric_medians[col]['median'] is not np.NAN)
                ,'isfloat':  (col in df_columns_numeric_medians and 'median' in df_columns_numeric_medians[col] and df_columns_numeric_medians[col]['median'] is not np.NAN
                              and 'max' in df_columns_numeric_medians[col] and myisnumeric(df_columns_numeric_medians[col]['max']) and (float(df_columns_numeric_medians[col]['max'])%1>0) )
                 }
            # clean_name = col.replace(".","_").replace("_id","id").replace("_class","class")[:31].upper()
            clean_name = clean_column_name(col,cleans_columns)
            # col = col[:31]
            cleans_columns[clean_name]=val

    # print('cleans_columns:',cleans_columns)
    return cleans_columns

##################################################################################
def new_column_sql_definition(col,clean_columns):
    col_val =  clean_columns[col]
    col_type=' varchar2('+str(max(col_val['length'],MIN_SIZE_VARCHAR))+' CHAR)'
    if col.startswith('JD_'):
        col_type=' numeric('+str(max(col_val['length'],MIN_SIZE_NUMBER))+')'
    elif col.startswith('GD_'):
        col_type=' timestamp'
    elif col_val['isnumeric']:
        # col_type=' numeric('+str(max(col_val['length'],MIN_SIZE_NUMBER))
        # if col_val['isfloat']:
        #     col_type+=",3"
        col_type=' varchar2('+str(max(col_val['length'],MIN_SIZE_NUMBER))+" CHAR"
        col_type+=')'
    return col+col_type
##################################################################################
def fix_table_diff(oracle_cursor,dest_table_name,clean_columns,dest_column_type='varchar(1000)'):
    oracle_cursor.execute("select * from "+dest_table_name+" where 1=0")
    exists_columns=list()
    # exists_columns = [ix[0] for ix in oracle_cursor.description]
    alter_columns_size_sql="alter table {0} modify (".format(dest_table_name)
    alter_columns_size=False
    for ix in oracle_cursor.description:
        col=ix[0].upper()
        exists_columns.append(col)
        if col in clean_columns :
            col_val =  clean_columns[col]
            if ix[2]<col_val['length']:
                alter_columns_size=True
                # new_col=col+((' numeric('+str(col_val['length'])+')') if col_val['isnumeric'] else (' varchar2('+str(col_val['length'])+' CHAR)'))+' ,'
                new_col=new_column_sql_definition(col,clean_columns)+' ,'
                alter_columns_size_sql=alter_columns_size_sql+new_col

    if alter_columns_size :
        alter_columns_size_sql=alter_columns_size_sql[:-1]+')'
        print('alter_columns_size_sql=',alter_columns_size_sql)
        oracle_cursor.execute(alter_columns_size_sql)

    # print("%s table exists columns:%s"%(dest_table_name,exists_columns))
    # for exist_column in exists_columns:

    new_columns = list(set([x.upper() for x in clean_columns])-set([x.upper() for x in exists_columns]))
    if new_columns:
        print("%s table new columns:%s"%(dest_table_name,new_columns))

        # new_clean_columns = clean_column_names(new_columns)
        # new_clean_columns = clean_column_name_length_isnumeric(new_columns)
        # alter_table_add_new_columns_query = "alter table  {0} add ({1} )".format(dest_table_name, (" "+dest_column_type+",").join(new_clean_columns).replace(".","_").replace("_id","id").replace("_class","class")+" "+dest_column_type)
        alter_table_add_new_columns_query = "alter table  {0} add (".format(dest_table_name)
        for col in new_columns:
            # new_col=col+(('numeric('+col.length+')') if col.isnumeric else ('varchar2('+col.length+')'))+' ,'
            col_val =  clean_columns[col]
            # new_col=col+((' numeric('+str(col_val['length'])+')') if col_val['isnumeric'] else (' varchar2('+str(col_val['length'])+' CHAR)'))+' ,'
            new_col=new_column_sql_definition(col,clean_columns)+' ,'
            alter_columns_size_sql=alter_columns_size_sql+new_col+" ,"
            alter_table_add_new_columns_query=alter_table_add_new_columns_query+new_col
        alter_table_add_new_columns_query=alter_table_add_new_columns_query[:-1]+')'

        print("fix_table_diff: %s table altered by :%s"% (dest_table_name,alter_table_add_new_columns_query) )
        oracle_cursor.execute(alter_table_add_new_columns_query)

##################################################################################
def create_table_if_not_exists(oracle_cursor,dest_table_name,clean_columns,dest_column_type='varchar(1000)',auto_gen_pk_name=None):
    try:
        oracle_cursor.execute("select * from "+dest_table_name+" where 1=0")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        if error.code == 942:
            # create_table_query = "create table  {0} ({1} )".format(dest_table_name, (" "+dest_column_type+",").join(clean_columns)+" "+dest_column_type)
            create_table_query = "create table  {0} (".format(dest_table_name)
            if auto_gen_pk_name:
                create_table_query=create_table_query+auto_gen_pk_name+" int generated as identity primary key,"
            print(clean_columns)
            for col in clean_columns:
                col_val=clean_columns[col]
                # str_col_len = col_val['length']+  (0 if col_val['length']<10 else (30 if col_val['length']<100 else (30 if col_val['length']<100 else 100)))
                # new_col=col+((' numeric('+str(col_val['length'])+')') if col_val['isnumeric'] else (' varchar2('+str(col_val['length'])+' CHAR)'))+' ,'
                new_col=new_column_sql_definition(col,clean_columns)+' ,'
                create_table_query=create_table_query+new_col
            create_table_query=create_table_query[:-1]+')'

            print("create_table_if_not_exists: %s table created by :%s"% (dest_table_name,create_table_query) )
            oracle_cursor.execute(create_table_query)
        else:
            raise e

##################################################################################
def add_jalali_dates(data,timestamp_column_names,timezone=pytz.timezone('Asia/Tehran')):
    for col in timestamp_column_names :
        if col in data:
            data['jd_'+col] = data[col].apply(lambda x: str(jdatetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%Y%m%d')) if x and str(x).lower()!='nan' else x )
##################################################################################
def add_gregorian_dates(data,timestamp_column_names,timezone=pytz.timezone('Asia/Tehran')):
    for col in timestamp_column_names :
        if col in data:
            # data['gd_'+col] = data[col].apply(lambda x: str(datetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%Y-%m-%d %H:%M:%S')) if x and str(x).lower()!='nan' else x )
            data['gd_'+col] = data[col].apply(lambda x: str(datetime.datetime.fromtimestamp(float(x)/1000).astimezone(timezone).strftime('%d-%b-%y %I:%M:%S %p')) if x and str(x).lower()!='nan' else x )
##################################################################################
def log_etl_failed(oracle_cursor,tablename,tableid,log):
    insert_query = "insert into log_etl_failed (tablename, tableid, log) values ('" \
                   +tablename \
                   +"','"+tableid \
                   +"','"+str(log).replace("'"," ") \
                   + "')"
    oracle_cursor.execute(insert_query)

##################################################################################
def get_subtables_name(subtables):
    subtables_name=[]
    for subtable in subtables:
        subtables_name.append(subtable["json_columnname"])
    # print("subtables names : %s"%(subtables_name))
    return subtables_name

##################################################################################
def create_subtables(df,subtables,oracle_cursor,dest_column_type='varchar2(1000)'):
    df_without_subtables = df
    if subtables:
        for subtable in subtables:
            # calculate subtable clean columns by getting all items
            subtable_all_objects=list()
            # for row in df[subtable["json_columnname"]].values.tolist():
            for i,row in df.iterrows():
                if subtable["json_columnname"] in df.columns:
                    subtable_str = str(df[subtable["json_columnname"]][i])
                    # print(subtable["json_columnname"],"=",subtable_str)
                    # items = yaml.load(row,yaml.FullLoader)
                    if subtable_str and str(subtable_str)!='nan':
                        try:
                            subtable_str = replace_datetime(subtable_str)
                            items = yaml.load(subtable_str,yaml.loader.UnsafeLoader)
                            for item in items:
                                for fk in subtable["foreignkeys"]:
                                    item[fk["foreignkey"]]=df[fk["key"]][i]

                            subtable_all_objects = subtable_all_objects+items
                        except Exception as err:
                            log_etl_failed(oracle_cursor,subtable["tablename"],df[subtable["master_pk"]][i],err)
            # subtable_df = pandas.DataFrame(subtable_all_objects)
            subtable_df = pandas.json_normalize(subtable_all_objects)

            ## create sub_subtables
            if "subtables" in subtable:
                create_subtables(subtable_df,subtable["subtables"],oracle_cursor,dest_column_type)
                # remove subtable columns from master
                subtables_name=get_subtables_name(subtable["subtables"])
                subtable_df = subtable_df[subtable_df.columns.difference(subtables_name)]


            # convert and add jalali dates
            if "timestamp2jalali" in subtable:
                add_jalali_dates(subtable_df,subtable["timestamp2jalali"])
            # convert and add gregorian dates
            if "timestamp2gregorian" in subtable:
                add_gregorian_dates(subtable_df,subtable["timestamp2gregorian"])

            #clean cloumns list
            subtable["clean_columns"] = clean_column_name_length_isnumeric(subtable_df)

            # create and fix subtable
            create_table_if_not_exists(oracle_cursor,subtable["tablename"],subtable["clean_columns"],dest_column_type,subtable["auto_gen_pk_name"])
            fix_table_diff(oracle_cursor,subtable["tablename"],subtable["clean_columns"],dest_column_type)

            subtable["df"]=subtable_df

##################################################################################
def trun_byte(src, byte_limit, encoding='utf-8'):
    if not isinstance(src,str):
        src=str(src)
    # if myisnumeric(src):
    #     src=str(src)
    if getsizeof(src)<byte_limit:
        return src
    else:
        return src.encode(encoding)[:byte_limit].decode(encoding, 'ignore')
##################################################################################
def replace_datetime(text:str,date_format='%Y/%m/%d %H:%M:%S'
                     ,invalid_date_pattern ="[datetime\.]*datetime\(\d{2,4}\s*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,6}\)\s*[: None]*"
                     ,fast_search="datetime("
                     ,replace_with_empty=": None"):
    if fast_search and fast_search not in text:
        return text
    founded_datetimes = re.findall(invalid_date_pattern,st)
    if founded_datetimes:
        converted_datetimes = list(map(lambda x:eval(x.replace(replace_with_empty,'')).strftime(date_format),founded_datetimes))
        splited_others = re.split(invalid_date_pattern,st)
        result=splited_others[0]
        for i in range(1,len(splited_others)):
            result+="'"+converted_datetimes[i-1]+"' "+splited_others[i]
        text=result
    return text
##################################################################################
def insert_subtables(oracle_cursor,subtables):
    subtables_counter=0
    if subtables:
        for subtable in subtables:
            t1= time.time()
            subtable_counter=0
            subtable_df = subtable["df"]
            for subtable_row in subtable_df.values.tolist():
                subtable_insert_query = "insert into "+subtable["tablename"]+"(" \
                                        +(", ").join(subtable["clean_columns"]) \
                                        +") values ('" \
                                        +("', '").join(trun_byte(v,4000).replace("'"," ") for v in subtable_row) \
                                        + "')"
                subtable_insert_query=subtable_insert_query.replace("'nan'","null")
                # print("subtable_insert_query=",subtable_insert_query)
                oracle_cursor.execute(subtable_insert_query)
                subtable_counter+=1
            print("%s records of %s subtable transfer completed at %s ms"%(subtable_counter,subtable["json_columnname"],(time.time()-t1)))
            subtables_counter=subtables_counter+subtable_counter

            if "subtables" in subtable:
                count=insert_subtables(oracle_cursor,subtable["subtables"])
                subtables_counter=subtables_counter+count

    return subtables_counter

##################################################################################

def etl_append(mongoDB,mongo_collection,mongo_query,oracleDB
               ,dest_table_name
               ,mongo_limit=0
               ,mongo_sort='creationdate'
               ,dest_column_type='varchar2(1000)'
               ,fn_clean_data=None
               ,timestamp2jalali=None
               ,timestamp2gregorian=None
               ,subtables=None):
    t1= time.time()
    print("etl_append: mongo_query=db.%s.find(%s).sort([{%s: 1}]).limit(%s); dest_table_name=%s start at : %s"%(mongo_collection,mongo_query,mongo_sort,mongo_limit,dest_table_name,t1))


    if mongo_limit>0:
        db_rows = mongoDB[mongo_collection].find(mongo_query).sort(mongo_sort).limit(mongo_limit)
    else:
        db_rows = mongoDB[mongo_collection].find(mongo_query)
    rows=list(db_rows)
    print("total readed records:%s"%(len(rows)))

    df=pandas.json_normalize(rows)
    df = df.applymap(lambda x: str(x).replace("'",""))
    print("data converted to string and removed single qoutes.")

    if timestamp2jalali:
        add_jalali_dates(df,timestamp2jalali)

    if timestamp2gregorian:
        add_gregorian_dates(df,timestamp2gregorian)

    oracle_cursor = oracleDB.cursor()
    # print("Database version:", oracleDB.version)

    if fn_clean_data and callable(fn_clean_data) :
        fn_clean_data(df)
        # print("data cleaned.")

    df_without_subtables = df
    if subtables:
        create_subtables(df,subtables,oracle_cursor,dest_column_type)
        subtables_name=get_subtables_name(subtables)
        df_without_subtables = df[df.columns.difference(subtables_name)]

    clean_columns = clean_column_name_length_isnumeric(df_without_subtables)


    create_table_if_not_exists(oracle_cursor,dest_table_name,clean_columns,dest_column_type)
    fix_table_diff(oracle_cursor,dest_table_name,clean_columns,dest_column_type)

    try:
        counter=0
        subtable_counter=0
        for row in df_without_subtables.values.tolist():
            # for i,row in df_without_subtables.iterrows():
            insert_query = "insert into "+dest_table_name+"(" \
                           +(", ").join(clean_columns) \
                           +") values ('" \
                           +("', '").join( trun_byte(v,4000).replace("'"," ") for v in row ) \
                           + "')"
            insert_query=insert_query.replace("'nan'","null")
            oracle_cursor.execute(insert_query)
            counter+=1

        print("%s records of %s table transfer completed at %s ms"%(counter,dest_table_name,(time.time()-t1)))

        subtable_counter+=insert_subtables(oracle_cursor,subtables)

        oracleDB.commit()
        print("commited %s master records and %s subtable records transfer completed at %s ms"%(counter , subtable_counter , (time.time()-t1)))
        return counter
    except Exception as err:
        print("Unexpected error:", err)
        if(insert_query):print("last_insert_query=",insert_query)
        if(subtable_insert_query):print("last_subtable_insert_query=",subtable_insert_query)
        if (oracleDB):
            oracleDB.rollback()
            raise err

##################################################################################
def _etl_update_between_lastmodified_lastcreated(mongoDB,mongo_collection
                                                 ,oracleDB,dest_table_name,dest_column_type='varchar2(1000)'
                                                 ,dest_column_4max='CREATIONDATE'
                                                 ,mongo_ordered_column='creationdate'
                                                 ,mongo_modification_column=None
                                                 ,chunk_size=1000
                                                 ,fn_clean_data=None,fn_cnv_max_value=None
                                                 ,timestamp2jalali=None
                                                 ,timestamp2gregorian=None
                                                 ,subtables=None):
    if not mongo_modification_column:
        return

    cursor=oracleDB.cursor()
    query="SELECT MAX("+dest_column_4max+") ,max("+mongo_modification_column+") FROM "+dest_table_name
    try:
        cursor.execute(query)
        result=cursor.fetchone()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        if error.code == 942:
            print(e)

    max_ordered_value=result[0] if result[0] else 0
    if fn_cnv_max_value and callable(fn_cnv_max_value):
        max_ordered_value=fn_cnv_max_value(max_ordered_value)

    max_modify_value=result[1] if result[1] else 0
    if fn_cnv_max_value and callable(fn_cnv_max_value):
        max_modify_value=fn_cnv_max_value(max_modify_value)

    mongo_query_modify={"$and":[{mongo_modification_column:{"$gt":max_modify_value}},{mongo_ordered_column:{"$lte":max_ordered_value}}]}
    print("etl_update_modified: query=%s => max_modify_value>%s, max_ordered_value<=%s, mongo_query=%s"%(query,max_modify_value, max_ordered_value,mongo_query_modify))
    chunk_size=0 # all changes in one chunk
    counter = etl_update(mongoDB,mongo_collection,mongo_query_modify,oracleDB,dest_table_name,chunk_size,mongo_ordered_column,dest_column_type,fn_clean_data,timestamp2jalali,timestamp2gregorian,subtables)

##################################################################################
def _etl_incremental(mongoDB,mongo_collection
                     ,oracleDB,dest_table_name,dest_column_type='varchar2(1000)'
                     ,dest_column_4max='CREATIONDATE'
                     ,mongo_ordered_column='creationdate'
                     ,chunk_size=1000
                     ,fn_clean_data=None,fn_cnv_max_value=None
                     ,timestamp2jalali=None
                     ,timestamp2gregorian=None
                     ,subtables=None):

    cursor=oracleDB.cursor()
    counter=1
    result=[0]
    while counter>0 :
        query="SELECT MAX("+dest_column_4max+") FROM "+dest_table_name
        try:
            cursor.execute(query)
            result=cursor.fetchone()
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 942:
                print(e)

        max_value=result[0] if result[0] else 0
        if fn_cnv_max_value and callable(fn_cnv_max_value):
            max_value=fn_cnv_max_value(max_value)

        # mongo_query={mongo_ordered_column:{"$gt":max_value}}
        mongo_query= {mongo_ordered_column:{"$gt":max_value}} if result[0] else {}
        print("etl_incremental: query=%s => max_value=%s, mongo_query=%s"%(query, max_value,mongo_query))

        counter = etl_append(mongoDB,mongo_collection,mongo_query,oracleDB,dest_table_name,chunk_size,mongo_ordered_column,dest_column_type,fn_clean_data,timestamp2jalali,timestamp2gregorian,subtables)

##################################################################################
def etl_from_to_jalali_date(mongoDB,mongo_collection,jalali_from,jalali_to
                            ,oracleDB,dest_table_name,dest_column_type='varchar2(1000)'
                            ,fn_clean_data=None):

    # mongo_query={mongo_ordered_column:{"$gte":max_value}}

    # counter = etl_append(mongoDB,mongo_collection,mongo_query,oracleDB,dest_table_name,dest_column_type=dest_column_type,fn_clean_data=fn_clean_data)
    pass

##################################################################################
def etl_incremental(mongo_uri,mongo_db,mongo_collection
                    ,oracle_dsn,oracle_user,oracle_pass
                    ,dest_table_name,dest_column_type='varchar2(1000)'
                    ,dest_column_4max='CREATIONDATE'
                    ,mongo_ordered_column='creationdate'
                    ,mongo_modification_column=None
                    ,chunk_size=1000
                    ,init_oracle_client_path='d:/app/oracle_instantclient_19_10'
                    ,fn_clean_data=None,fn_cnv_max_value=None
                    ,timestamp2jalali=None
                    ,timestamp2gregorian=None
                    ,subtables=None):
    print("transfering data from mongodb[%s,%s,%s] to oracle[%s] starting at :%s "%(mongo_db,mongo_collection,chunk_size,dest_table_name,time.gmtime()))

    if init_oracle_client_path :
        try:
            cx_Oracle.init_oracle_client(init_oracle_client_path)
        except Exception as ex:
            print(ex)

    t1= time.time()

    mongoClient = MongoClient(mongo_uri)
    mongoDB = mongoClient[mongo_db]

    oracleDB = cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn)

    if mongo_modification_column:
        _etl_update_between_lastmodified_lastcreated(mongoDB,mongo_collection,oracleDB,dest_table_name,dest_column_type,dest_column_4max,mongo_ordered_column,mongo_modification_column,chunk_size,fn_clean_data,fn_cnv_max_value,timestamp2jalali,timestamp2gregorian,subtables)

    _etl_incremental(mongoDB,mongo_collection,oracleDB,dest_table_name,dest_column_type,dest_column_4max,mongo_ordered_column,chunk_size,fn_clean_data,fn_cnv_max_value,timestamp2jalali,timestamp2gregorian,subtables)

    oracleDB.close()

##################################################################################
def etl(mongo_uri,mongo_db,mongo_collection,mongo_query,oracle_dsn,oracle_user,oracle_pass
        ,dest_table_name
        ,mongo_limit=0
        ,mode='append'
        ,dest_column_type='varchar2(1000)'
        ,init_oracle_client_path='/Users/adel/ds/oracle_instantclient_19_8'
        ,chunk_size=1000
        ,fn_clean_data=None):
    # MONGO_URI = "mongodb://ops:ops%402020@172.16.27.11:27018/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
    # MONGO_DB = "credit_mng_db"
    # MONGO_QUERY=query_match_all
    # TABLE_NAME= 'credit_contracts'
    # # COLUMN_TYPE="text"
    # COLUMN_TYPE="varchar(2000)"
    # ORACLE_USER = "stage"
    # ORACLE_PASS = "stage"
    # ORACLE_DSN = "172.18.24.84/orcl"

    print("transfering data from mongodb[%s,%s,%s,%s] to oracle[%s] starting at :%s "%(mongo_db,mongo_collection,mongo_query,mongo_limit,dest_table_name,time.gmtime()))

    # DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 1): image not found"
    # cx_Oracle.init_oracle_client("/Users/adel/ds/oracle_instantclient_19_8")
    if init_oracle_client_path :
        cx_Oracle.init_oracle_client(init_oracle_client_path)

    # t1= int(round(time.time() * 1000))
    t1= time.time()

    mongoClient = MongoClient(mongo_uri)
    mongoDB = mongoClient[mongo_db]

    # db_rows = mongoDB.contracts.find(MONGO_QUERY).limit(1)
    if mongo_limit>0:
        db_rows = mongoDB[mongo_collection].find(mongo_query).limit(mongo_limit)
    else:
        db_rows = mongoDB[mongo_collection].find(mongo_query)
    rows=list(db_rows)
    print("total readed records:%s"%(len(rows)))

    data=pandas.json_normalize(rows)
    # data = data.applymap(lambda x: str(x).replace("\"[","").replace("\"]",""))
    data = data.applymap(lambda x: str(x).replace("'",""))
    print("data converted to string and removed single qoutes.")
    # data["_id"] = data["_id"].apply(lambda x: str(x))
    # print(data,type(data),data.columns)

    if fn_clean_data and callable(fn_clean_data) :
        fn_clean_data(data)
        print("data cleaned.")


    try:
        clean_columns = clean_column_names(data.columns)
        # create_query = "create table  {0} ({1} )".format(dest_table_name, (" "+dest_column_type+",").join(clean_columns).replace(".","_").replace("_id","id").replace("_class","class")+" "+dest_column_type)
        create_query = "create table  {0} ({1} )".format(dest_table_name, (" "+dest_column_type+",").join(clean_columns)+" "+dest_column_type)
        # create_query = "create table  {0} ( ".format(dest_table_name)
        # for col in data.columns :
        #     col = col.replace(".","_").replace("_id","id").replace("_class","class")
        #     col = col[:31]
        #     create_query += (" "+dest_column_type+",")+col
        # create_query += +" "+dest_column_type+")"

        # print(create_query)
        # delete_query = "delete from "+TABLE_NAME
        drop_query = "drop table "+dest_table_name
        delete_query = "delete from "+dest_table_name
        # insert_query = "insert into {0} ({1}) values (?{2})".format(TABLE_NAME, ",".join(data.columns).replace(".","_").replace("_id","id").replace("_class","class"), ",?" * (len(data.columns)-1))
        bind_names = ",".join(":" + str(i + 1) \
                              for i in range(len(clean_columns)))
        insert_query = "insert into "+dest_table_name+"(" \
                       +(", ").join(clean_columns) \
                       +") values (" + bind_names + ")"
        # print(insert_query)

        db = cx_Oracle.connect(oracle_user, oracle_pass, oracle_dsn)
        print("Database version:", db.version)
        # print("Successfully Connected to oracle")
        c = db.cursor()
        if mode=="delete":
            c.execute(delete_query)
            print("%s table deleted "%dest_table_name)
        if mode=="drop":
            c.execute(drop_query)
            print("%s table droped "%dest_table_name)
        if mode=="drop" or mode=="create":
            print(create_query)
            c.execute(create_query)
            print("%s table created by :%s"% (dest_table_name,create_query) )
        if mode=="append" or mode=="delete":
            c.execute("select * from "+dest_table_name+" where 1=0")
            exists_columns = [ix[0] for ix in c.description]
            # print("%s table exists columns:%s"%(dest_table_name,exists_columns))
            new_columns = list(set([x.upper() for x in clean_columns])-set([x.upper() for x in exists_columns]))
            if new_columns:
                print("%s table new columns:%s"%(dest_table_name,new_columns))
                new_clean_columns = clean_column_names(new_columns)
                alter_table_add_new_columns_query = "alter table  {0} add ({1} )".format(dest_table_name, (" "+dest_column_type+",").join(new_clean_columns)+" "+dest_column_type)
                c.execute(alter_table_add_new_columns_query)
                print("%s table altered by :%s"% (dest_table_name,alter_table_add_new_columns_query) )

        # print(data.values.tolist())
        counter=0

        for row in data.values.tolist():
            # print(row)
            # c.execute(insert_query , row)
            insert_query = "insert into "+dest_table_name+"(" \
                           +(", ").join(clean_columns) \
                           +") values ('" \
                           +("', '").join(trun_byte(v,4000).replace("'"," ") for v in row) \
                           + "')"
            # print(insert_query)
            c.execute(insert_query)

            counter+=1
            if chunk_size>0 and counter%chunk_size==0:
                db.commit()
                print("%s records transfered at %s ms"%(counter , (time.time()-t1)))
        # values.clear()
        db.commit()
        c.close()
        print("%s records transfer completed at %s ms"%(counter , (time.time()-t1)))
    except Exception as err:
        print("Unexpected error:", err)
        if(insert_query):print("last_insert_query=",insert_query)
        if(subtable_insert_query):print("last_subtable_insert_query=",subtable_insert_query)
        if (db):
            db.rollback()
            raise err
    finally:
        if (db):
            db.close()
            print("The connection is closed")




##################################################################################

def etl_update(mongoDB,mongo_collection,mongo_query_modify,oracleDB
               ,dest_table_name
               ,mongo_limit=0
               ,mongo_sort='creationdate'
               ,dest_column_type='varchar2(1000)'
               ,fn_clean_data=None
               ,timestamp2jalali=None
               ,timestamp2gregorian=None
               ,subtables=None
               ,mongo_pkfield="_id"
               ,oracle_pkfield="ID"
               ):
    t1= time.time()
    print("etl_update: mongo_query_modify=db.%s.find(%s).sort([{%s: 1}]).limit(%s); dest_table_name=%s start at : %s"%(mongo_collection,mongo_query_modify,mongo_sort,mongo_limit,dest_table_name,t1))


    if mongo_limit>0:
        db_rows = mongoDB[mongo_collection].find(mongo_query_modify).sort(mongo_sort).limit(mongo_limit)
    else:
        db_rows = mongoDB[mongo_collection].find(mongo_query_modify)
    rows=list(db_rows)
    print("total readed records for update:%s"%(len(rows)))

    df=pandas.json_normalize(rows)
    df = df.applymap(lambda x: str(x).replace("'",""))
    print("data converted to string and removed single qoutes.")

    if timestamp2jalali:
        add_jalali_dates(df,timestamp2jalali)

    if timestamp2gregorian:
        add_gregorian_dates(df,timestamp2gregorian)

    oracle_cursor = oracleDB.cursor()
    # print("Database version:", oracleDB.version)

    if fn_clean_data and callable(fn_clean_data) :
        fn_clean_data(df)
        # print("data cleaned.")

    if subtables:
        create_subtables(df,subtables,oracle_cursor,dest_column_type)
        subtables_name=get_subtables_name(subtables)
        df_without_subtables = df[df.columns.difference(subtables_name)]

    # clean_columns = clean_column_names(df.columns)
    clean_columns = clean_column_name_length_isnumeric(df_without_subtables)
    # columns_length = get_columns_type_length(df)

    create_table_if_not_exists(oracle_cursor,dest_table_name,clean_columns,dest_column_type)
    fix_table_diff(oracle_cursor,dest_table_name,clean_columns,dest_column_type)

    try:
        counter=0
        subtable_counter=0
        for row in df_without_subtables.values.tolist():
            # for i,row in df_without_subtables.iterrows():
            clean_columns_key_list = list(clean_columns)
            update_query = "update "+dest_table_name+" set " \
                           +(", ").join( (clean_columns_key_list[i]+"='"+(trun_byte(row[i],4000).replace("'"," "))+"'") for i in range(len(clean_columns_key_list))) \
                           +" where "+oracle_pkfield+"='"+df_without_subtables[mongo_pkfield][counter]+"'"
            update_query=update_query.replace("'nan'","null")
            oracle_cursor.execute(update_query)
            counter+=1

        print("%s records of %s table transfer completed at %s ms"%(counter,dest_table_name,(time.time()-t1)))

        delete_subtables(oracle_cursor,subtables)
        subtable_counter+=insert_subtables(oracle_cursor,subtables)

        oracleDB.commit()
        print("commited %s master records and %s subtable records transfer completed at %s ms"%(counter , subtable_counter , (time.time()-t1)))
        return counter
    except Exception as err:
        print("Unexpected error:", err)
        if(update_query):print("last_update_query=",update_query)
        if(subtable_insert_query):print("last_subtable_insert_query=",subtable_insert_query)
        if (oracleDB):
            oracleDB.rollback()
            raise err

##################################################################################
def delete_subtables(oracle_cursor,subtables):
    if subtables:
        for subtable in subtables:
            t1= time.time()
            subtable_df = subtable["df"]
            if len(subtable_df)>0:
                delete_column_values = subtable_df[subtable["delete_column"]].unique()
                if len(delete_column_values)>0:
                    subtable_delete_query = "delete "+subtable["tablename"]+" where " \
                                            +subtable["delete_column"]+" in ('" \
                                            +("','").join(delete_column_values) \
                                            +"')"
                    # subtable_insert_query=subtable_insert_query.replace("'nan'","null")
                    # print("subtable_insert_query=",subtable_insert_query)
                    oracle_cursor.execute(subtable_delete_query)
                print("delete on %s subtable by %s completed at %s ms"%(subtable["json_columnname"],subtable["delete_column"],(time.time()-t1)))

            if "subtables" in subtable:
                delete_subtables(oracle_cursor,subtable["subtables"])

##################################################################################
