import requests
import pprint

####GET
query={
    "query": {
        "match": {"status":0}
    }
    ,"fields": ["oid", "creationDate","datetime","datetime2"]
    ,"_source": False
}
    #"_source": false
resp = requests.post('http://localhost:9200/activities_test2/_search',json=query)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))
#print("text=",resp.text)
# pprint("content=",resp.content)
# print("headers=",resp.headers)
# print("cookies=",resp.cookies)
#print("history=",resp.history)
pprint.pprint(resp.json())


# ###POST
# task = {"summary": "Take out trash", "description": "" }
# resp = requests.post('https://todolist.example.com/tasks/', json=task)
# if resp.status_code != 201:
#     raise ApiError('POST /tasks/ {}'.format(resp.status_code))
# print('Created task. ID: {}'.format(resp.json()["id"]))
#
#
# ######HEADERS
# # The shortcut
# resp = requests.post('https://todolist.example.com/tasks/', json=task)
# # The equivalent longer version
# resp = requests.post('https://todolist.example.com/tasks/',
#                      data=json.dumps(task),
#                      headers={'Content-Type':'application/json'},
#
