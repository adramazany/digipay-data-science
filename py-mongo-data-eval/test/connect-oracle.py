import cx_Oracle

user = "mongodb"
pw = "Mongo123"
# dsn = "172.18.24.84/orclpdb1"
dsn = "172.18.24.84/orcl"

# DPI-1047: Cannot locate a 64-bit Oracle Client library: "dlopen(libclntsh.dylib, 1): image not found"
cx_Oracle.init_oracle_client("/Users/adel/ds/oracle_instantclient_19_8")
con = cx_Oracle.connect(user, pw, dsn)
print("Database version:", con.version)

cursor=con.cursor()
cursor.execute("select * from psp")
records=cursor.fetchall();
for r in records :
    print (r)

cursor.close()
con.close()