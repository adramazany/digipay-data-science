import requests
import pprint

query={
    "aggs": {
        "total_by_date_year": {
            "terms": {
                "field": "src_owner_cellNumber.keyword"
            },
            "aggs": {
                "last_datetime": {"max": {"field": "datetime"}}
                ,"last_jalali_date": {"max": {"field": "jalali_date"}}
            }
        }
    }
}

resp = requests.post('http://localhost:9200/activities_test/_search?size=0',json=query)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

