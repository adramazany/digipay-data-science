import requests
import pprint

count_all={
    "query": {
        "match_all":{}
    }}
#activities_test    =2,052,906
#activities         =76,083,864

count_empty_jalali_date={
    "query": {
        "bool":{"must_not":[
            {"exists":{"field":"jalali_date"}}
        ]}}}
#activities_test    =0
#activities         =73,068,604

count_empty_src_cellNumber={
    "query": {
        "bool":{"must_not":[
            {"exists":{"field":"src_owner_cellNumber"}}
        ]}}}
#activities_test    =0
#activities         =284,069

count_empty_src_username={
    "query": {
        "bool":{"must_not":[
            {"exists":{"field":"src_owner_username"}}
        ]}}}
#activities_test    =0
#activities         =284,069

count_empty_creationDate={
    "query": {
        "bool":{"must_not":[
            {"exists":{"field":"creationDate"}}
        ]}}}
#activities_test    =0
#activities         =0

resp = requests.post('http://localhost:9200/activities/_count',json=count_empty_src_username)
if resp.status_code != 200:
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

#1768