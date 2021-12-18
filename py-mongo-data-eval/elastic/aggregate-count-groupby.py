import requests
import pprint

total_by_jalali_year={
    "aggs": {
        "total_by_jalali_year": {
            "terms": {
                "field": "jalali_year"
            },
            "aggs": {
                "total_count": {"sum": {"field": "count"}}
                ,"total_amount": {"sum": {"field": "amount"}}
                ,"total_feeCharge": {"sum": {"field": "feeCharge"}}
            }
        }
    }
    ,"_source":False
}
total_by_jalali_month={
    "aggs": {
        "total_by_jalali_month": {
            "terms": {
                "field": "jalali_month"
            },
            "aggs": {
                "total_count": {"sum": {"field": "count"}}
                ,"total_amount": {"sum": {"field": "amount"}}
                ,"total_feeCharge": {"sum": {"field": "feeCharge"}}
            }
        }
    }
    ,"_source":False
}
grafana_query={
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "datetime": {
                            "gte": 1583249310719,
                            "lte": 1588748447969,
                            "format": "epoch_millis"
                        }
                    }
                },
                {
                    "query_string": {
                        "analyze_wildcard": True,
                        "query": "type:112"
                    }
                }
            ]
        }
    },
    "aggs": {
        "2": {
            "terms": {
                "field": "jalali_day",
                "size": 31,
                "order": {
                    "_count": "desc"
                },
                "min_doc_count": 0,
                "missing": "0"
            },
            "aggs": {}
        }
    }
}


ES_INDEX="activities2"
# ES_QUERY=total_by_jalali_year
# ES_QUERY=total_by_jalali_month
ES_QUERY=grafana_query

resp = requests.post('http://localhost:9200/'+ES_INDEX+'/_search?size=0',json=ES_QUERY)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

