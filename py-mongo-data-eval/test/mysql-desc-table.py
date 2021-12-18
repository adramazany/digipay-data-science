import mysql.connector

mydb = mysql.connector.connect(
    host="172.16.27.11",
    port="3307",
    user="ops",
    password="ops@2020"
)
print(mydb)

cursor=mydb.cursor()
cursor.execute("desc switch_credit_db.users")
records=cursor.fetchall();
for r in records :
    print (r)

cursor.close()
mydb.close()