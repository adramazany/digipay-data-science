import requests
import datetime
import time
import sqlite3
from os.path import expanduser

query_match_all = {
    "query": {
        "match_all": {}
    }
}


def elastic_execute_query(query,index):
    resp = requests.post('http://localhost:9200/'+index+'/_search?size=10000',json=query)
    if resp.status_code != 200:
        print("resp=",resp.content)
        raise Exception('POST {}'.format(resp.status_code))
    # print("elastic_execute_query",resp.json())
    return resp.json()


ES_SERVER="localhost:9200"
ES_QUERY_INDEX= "budget_daily_ipg"
DB_URL= expanduser("~")+'/ds/datafile/'+ES_QUERY_INDEX+'.db'
TABLE_NAME= ES_QUERY_INDEX
ES_QUERY=query_match_all

t1= int(round(time.time() * 1000))

json_data = elastic_execute_query(ES_QUERY, ES_QUERY_INDEX)['hits']['hits']

db=None
try:
    columns = []
    column = []
    print("json_data",json_data)
    for data in json_data:
        print("data",data)
        column = list(data['_source'].keys())
        print("column",column)
        for col in column:
            if col not in columns:
                columns.append(col)

    #Here we get values of the columns in the JSON file in the right order.
    value = []
    values = []
    for data in json_data:
        for i in columns:
            value.append(str(dict(data['_source']).get(i)))
        values.append(list(value))
        value.clear()

    #Time to generate the create and insert queries and apply it to the sqlite3 database
    create_query = "create table if not exists {0} ({1})".format(TABLE_NAME, " text,".join(columns))
    delete_query = "delete from "+TABLE_NAME
    insert_query = "insert into {0} ({1}) values (?{2})".format(TABLE_NAME, ",".join(columns), ",?" * (len(columns)-1))

    print("insert has started at " + str(datetime.datetime.now()))
    db = sqlite3.connect(DB_URL)
    print("Successfully Connected to SQLite")
    c = db.cursor()
    c.execute(create_query)
    c.execute(delete_query)
    c.executemany(insert_query , values)
    values.clear()
    db.commit()
    c.close()
    print("insert has completed at " + str(datetime.datetime.now()))

except sqlite3.Error as error:
    print("Failed to insert data into sqlite table", error)
finally:
    if (db):
        db.close()
        print("The SQLite connection is closed")
