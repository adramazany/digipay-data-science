import requests
import pprint

count_empty_cellNumber={
    "query": {
        "bool":{"must":[
            {"exists":{"field":"src_owner_cellNumber"}}
    ]}}}
#1768
count_empty_username={
    "query": {
        "exists": {"field": "src_owner_username"}
    }
}
#1768

resp = requests.post('http://localhost:9200/activities_test2/_count',json=count_empty_cellNumber)
if resp.status_code != 200:
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

#1768