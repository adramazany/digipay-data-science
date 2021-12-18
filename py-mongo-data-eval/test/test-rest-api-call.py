import requests
import pprint

####GET
resp = requests.get('http://localhost:9200/activities/_count')
if resp.status_code != 200:
    # This means something went wrong.
    raise Exception('GET /tasks/ {}'.format(resp.status_code))
print("text=",resp.text)
print("content=",resp.content)
print("headers=",resp.headers)
print("cookies=",resp.cookies)
#print("history=",resp.history)
print("json=",resp.json())


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
