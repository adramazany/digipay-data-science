import requests
import pprint

count_docs_has_product={
    "query": {
        "bool": {
            "should": [
                {"exists":{"field": "is_app"}}
                ,{"exists":{"field": "is_ipg"}}
                ,{"exists":{"field": "is_wallet"}}
            ]}}}
count_docs_not_have_product={
    "query": {
        "bool": {
            "must_not": [
                {"exists":{"field": "is_app"}}
                ,{"exists":{"field": "is_ipg"}}
                ,{"exists":{"field": "is_wallet"}}
            ]}}}
count_docs_maybe_have_product={
    "query": {
        "bool": {
            "must": {
                "terms":{
                    "type":[0,1,12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]
                }}}}}

count_docs_should_have_product_not_have={
    "query": {
        "bool": {
            "must": {
                "terms":{
                    "type":[0,1,12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]
                }}
            ,"must_not": [
                {"exists":{"field": "is_app"}}
                ,{"exists":{"field": "is_ipg"}}
                ,{"exists":{"field": "is_wallet"}}
            ]
        }}}

resp = requests.post('http://localhost:9200/activities/_count',json=count_docs_should_have_product_not_have)
if resp.status_code != 200:
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())
