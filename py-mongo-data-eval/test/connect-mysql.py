import mysql.connector

# mydb = mysql.connector.connect(
#     host="localhost",
#     user="gl",
#     password="gl"
# )
mydb = mysql.connector.connect(
    host="10.198.110.63",
    user="ops",
    password="ops@2020"
)
print(mydb)

cursor=mydb.cursor()
cursor.execute("select * from db_user_mng.businesses limit 10")
records=cursor.fetchall();
for r in records :
    print (r)

cursor.close()
mydb.close()
