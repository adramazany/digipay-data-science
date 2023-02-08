"""config.py:
    Provide all configuration and decisions of the how to run for application
"""
__author__      = "Adel Ramezani <adramazany@gmail.com>"

host='0.0.0.0'
port=5000

etl_scheduler_interval_minutes=10      # 0 means disable scheduler

### connect to remove hive
# hive_metastore_uris="thrift://localhost:9083"
hive_metastore_uris=None

### using apache spark storage
# spark_sql_table_prefix="parquet."
spark_sql_table_prefix=""

### using hive storage,
# spark_sql_warehouse_dir="./spark-warehouse"
spark_sql_warehouse_dir=None

### This can be a mesos:// or spark:// URL, yarn to run on YARN, and "local" to run locally with one thread, or local[N] to run locally with N threads.
master="local[*]"

### spark display name
appName="DP-c2c-bet-fraud-detection"

### source input format
# read_format="json"
read_format="com.crealytics.spark.excel"
read_excel_format="com.crealytics.spark.excel"
read_excel_options= {"header":"true","inferSchema":"true"}

# read_excel_required_columns= ["pan_src", "pan_dest", "transactiontype", "terminaltype", "date", "time", "rrn", "amount", "merchantno", "terminalno", "bank_issuer", "psp", "channel", "mobile_app", "cellnumber"]
read_excel_required_columns= ["pan_src", "pan_dest", "date", "time", "rrn", "amount", "merchantno", "terminalno", "bank_issuer", "psp", "channel", "cellnumber"]

### source input files filter
# pathGlobFilter="*.json"

### define write mode "append" is default in some test case I'll use "overwrite"
write_mode="append"

### define write format
write_format="parquet"

### source input paths, these help changing specified path in test cases
# searches_src_path="./DataLake/searches"
# visitors_src_path="./DataLake/visitors"
c2cbet_src_path="./fraud-data/"

orcl_172_props= {
    "url":"jdbc:oracle:thin:@172.18.24.84:1521/ORCL"
    ,"user":"mongodb"
    ,"password":"Mongo123"
    ,"driver":"oracle.jdbc.OracleDriver"
}

orcl_10_props= {
    "url":"jdbc:oracle:thin:@10.198.31.51:1521/dgporclw"
    ,"user":"mongodb"
    ,"password":"OraMon123"
    ,"driver":"oracle.jdbc.OracleDriver"
}

derby_props= {
    "url":"jdbc:derby:c2cbet;create=true"
    ,"driver":"org.apache.derby.jdbc.EmbeddedDriver"
}

db_probs = orcl_10_props

# db_properties['url']="oracle+cx_oracle://mongodb:Mongo123@172.18.24.84:1521/?service_name=ORCL"
oracle_client_path='d:/app/oracle_instantclient_19_10'
