# mongo2oracle etl tools is (c) 2021 Adel Ramezani <adramazany at gmail.com>.
# The mongo2oracle etl tools module was contributed to Python as of Python 3.8 and thus
# was licensed under the Python license. Same license applies to all files in
# the mongo2oracle package project.
from mongo2oracle import mongo2oracle, mongo2oracle_v1


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
    mongo2oracle.etl_incremental(mongo_uri,mongo_db,mongo_collection
                                 ,oracle_dsn,oracle_user,oracle_pass
                                 ,dest_table_name,dest_column_type
                                 ,dest_column_4max
                                 ,mongo_ordered_column
                                 ,mongo_modification_column
                                 ,chunk_size
                                 ,init_oracle_client_path
                                 ,fn_clean_data,fn_cnv_max_value
                                 ,timestamp2jalali
                                 ,timestamp2gregorian
                                 ,subtables)