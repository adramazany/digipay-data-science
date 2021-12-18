import requests
import pprint

query={
    "aggs": {
        "group_src_cellNumber": {
            "terms": {
                "field": "src_owner_cellNumber"
            },
            "aggs": {
                "datetime_last": {
                    "terms": {
                        "field": "datetime"
                    }
                    ,"aggs":{
                        "group_docs":{
                            "top_hits":{
                                "size":1
                                ,"sort":[{
                                    "collected":{
                                        "order":"desc"
                                    }
                                }]
                            }
                        }
                    }
                }
            }
        }
    }
}

resp = requests.post('http://localhost:9200/activities_test/_search?size=0',json=query)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

