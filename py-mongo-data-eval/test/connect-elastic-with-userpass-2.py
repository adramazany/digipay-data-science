import requests
from requests.auth import HTTPBasicAuth
from elasticsearch import Elasticsearch, helpers
import pprint

query_match_all={
    "query":{
        "match_all":{}
    }
}

ES_USER = 'elastic'
ES_PASS = 'digipay'

query=query_match_all
es = Elasticsearch(http_auth=(ES_USER,ES_PASS))
result = helpers.scan(es, query=query,index="budget_daily_ipg")
for doc in result:
    pprint.pprint(doc)
