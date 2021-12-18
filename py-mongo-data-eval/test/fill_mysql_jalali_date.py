import mysql.connector
import jdatetime
import datetime

mydb = mysql.connector.connect(
    host="10.198.111.61",
    user="digipay",
    password="D1giP@y_Us3r_MNG"
)
print(mydb)
cursor=mydb.cursor()

START_DATE=jdatetime.date(1397, 7, 26)
END_DATE=jdatetime.date(1400, 12, 29)
jdate = START_DATE
while jdate<=END_DATE:
    gdate=jdate.togregorian()
    sql="insert into jalali_date (gdate,jdate)values('%s','%s');"%(gdate,jdate.strftime("%Y/%m/%d"))
    print(sql)
    # cursor.execute()
    jdate=jdate+datetime.timedelta(days=1)


cursor.execute("select * from general_ledger_db.jalali_date limit 10")
records=cursor.fetchall();
for r in records :
    print (r)

cursor.close()
mydb.close()
