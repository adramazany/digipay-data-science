import requests
from requests.auth import HTTPBasicAuth

query_match_all={
    "query":{
        "match_all":{}
    }
}
ES_USER = 'elastic'
ES_PASS = 'digipay'

resp = requests.get("http://localhost:9200/activities2",json=query_match_all,auth=HTTPBasicAuth(ES_USER, ES_PASS))
if resp.status_code!=200:
    print(resp.content)
    raise Exception('POST {}'.format(resp.status_code))
print(resp.json())