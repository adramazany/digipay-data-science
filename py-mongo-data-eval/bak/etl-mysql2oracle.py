import sys
import petl
import mysql.connector
import psycopg2
import sqlalchemy
import cx_Oracle
import pandas
import time
import mysql.connector

MYSQL_QUERY="select * from switch_credit_db.users where id>=0  and id<14000000 order by id "
TABLE_NAME="SWITCH_CREDIT_USERS"

MYSQL_DSN='mysql+mysqlconnector://ops:ops%402020@172.16.27.11:3307/switch_credit_db'
MYSQL_HOST="172.16.27.11"
MYSQL_PORT="3307"
MYSQL_USER="ops"
MYSQL_PASSWORD="ops@2020"
ORACLE_USER = "stage"
ORACLE_PASS = "stage"
ORACLE_DSN_FULL = "oracle+cx_oracle://stage:stage@172.18.24.84/orcl"
ORACLE_DSN = "172.18.24.84/orcl"
CHUNK_SIZE=1000

t1= time.time()

cx_Oracle.init_oracle_client("/Users/adel/ds/oracle_instantclient_19_8")

try:
    # my_eng = sqlalchemy.create_engine(MYSQL_DSN)
    # my_cnx = my_eng.connect()
    # # pg_eng = sqlalchemy.create_engine('postgresql+psycopg2://postgres:postgres@localhost/hr')
    # # pg_cnx = pg_eng.connect()
    # orcl_eng = sqlalchemy.create_engine(ORACLE_DSN)
    # orcl_cnx = orcl_eng.connect()
    # my_staff = petl.fromdb(my_cnx, MYSQL_QUERY)
    # orcl_eng.execute("DROP TABLE "+TABLE_NAME)
    # orcl_eng.execute("CREATE TABLE "+TABLE_NAME+" ( ID INT PRIMARY KEY) ")
    #
    # petl.todb(my_staff, orcl_cnx, TABLE_NAME)

    # src = sqlalchemy.create_engine(MYSQL_DSN)
    # dst = sqlalchemy.create_engine(ORACLE_DSN_FULL)
    # df = pandas.read_sql(MYSQL_QUERY, src)
    # print("read time=",time..timestamp()*1000, " now+04:30 = ",d2.timestamp()*1000, " +04:30 = ",(d2.timestamp()-d1.timestamp())*1000)
    # print(df)
    # df = df.applymap(lambda x: str(x).replace("'","").replace("'","").replace("NaT",""))

    # df.to_sql(TABLE_NAME, dst,if_exists = 'append',index=False,chunksize=CHUNK_SIZE)

    mydb = mysql.connector.connect(
        host    =MYSQL_HOST    ,
        port    =MYSQL_PORT    ,
        user    =MYSQL_USER    ,
        password=MYSQL_PASSWORD
    )
    print(mydb)

    my_cur=mydb.cursor()
    my_cur.execute(MYSQL_QUERY)
    records=my_cur.fetchall();
    print("all fetched data=",len(records),(time.time()-t1))

    my_cur.close()
    mydb.close()

    print(records[0])

    bind_names = ",".join(":" + str(i + 1) \
                          for i in range(len(records[0])))
    insert_query = "insert into "+TABLE_NAME+" values (" + bind_names + ")"
    print(insert_query)

    print("insert has started at " + str(time.time()))
    db = cx_Oracle.connect(ORACLE_USER, ORACLE_PASS, ORACLE_DSN)
    print("Database version:", db.version)
    print("Successfully Connected to Oracle")
    c = db.cursor()
    # print(data.values.tolist())
    counter=0
    # for row in df.values.tolist():
    for row in records:
        # print(row)
        c.execute(insert_query , row)
        counter+=1
        if counter%CHUNK_SIZE==0:
            db.commit()
            print("commited counter:",counter,(time.time()-t1))
    # values.clear()
    db.commit()
    c.close()

    print("ETL of "+TABLE_NAME+" succeed from mysql to oracle. duration="+str(time.time()-t1))
except sqlalchemy.exc.DataError as err:
    print("Unexpected error: {0}".format(err))
    raise
finally:
    if (mydb):
        mydb.close()
    if (db):
        db.close()
        print("The connection is closed")
    # orcl_cnx.close()
    # my_cnx.close()
    # dst.close()
    # src.close()


