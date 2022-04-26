""" config :
    4/24/2022 2:50 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

db_props_orcl_172= {
    "url":"jdbc:oracle:thin:@172.18.24.84:1521/ORCL"
    ,"user":"mongodb"
    ,"password":"Mongo123"
    ,"driver":"oracle.jdbc.OracleDriver"
}

db_props_orcl_10= {
    "url":"jdbc:oracle:thin:@10.198.31.51:1521/dgporclw"
    ,"user":"mongodb"
    ,"password":"OraMon123"
    ,"driver":"oracle.jdbc.OracleDriver"
}

db_props_derby= {
    "url":"jdbc:derby:c2cbet;create=true"
    ,"driver":"org.apache.derby.jdbc.EmbeddedDriver"
}

db_probs = db_props_orcl_10


oracle_client_path='d:/app/oracle_instantclient_19_10'
dest_db_url = 'oracle+cx_oracle://finance:finance@10.198.31.51:1521/?service_name=dgporclw'
