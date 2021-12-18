import requests
import pprint

query={"aggs": {
    "total_count":{ "sum": { "field": "count" } }
    ,"total_amount":{ "sum": { "field": "amount" } }
    ,"total_fee":{ "sum": { "field": "feeCharge" } }
    }}
resp = requests.post('http://localhost:9200/activities_day_stat/_search?size=0',json=query)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

