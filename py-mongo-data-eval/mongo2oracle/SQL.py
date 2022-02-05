
class SQL:
    ##################################################################################
    def new_column_sql_definition(col,clean_columns):
        col_val =  clean_columns[col]
        col_type=' varchar2('+str(max(col_val['length'],MIN_SIZE_VARCHAR))+' CHAR)'
        if col.startswith('JD_'):
            col_type=' numeric(10)'
        elif col.startswith('GD_'):
            col_type=' timestamp'
        elif col_val['isnumeric']:
            # col_type=' numeric('+str(max(col_val['length'],MIN_SIZE_NUMBER))
            # if col_val['isfloat']:
            #     col_type+=",3"
            col_type=' varchar2('+str(max(col_val['length'],MIN_SIZE_NUMBER))
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
    def log_etl_failed(oracle_cursor,tablename,tableid,log):
        insert_query = "insert into log_etl_failed (tablename, tableid, log) values ('" \
                       +tablename \
                       +"','"+tableid \
                       +"','"+str(log).replace("'"," ") \
                       + "')"
        oracle_cursor.execute(insert_query)
