# POD get report
import json

import requests
from requests.structures import CaseInsensitiveDict
from requests_oauthlib import OAuth2Session

pod_client={"url":"https://dashboardservice.pod.ir/v1/userreport/1414/getCSV?forceSave=false"
    ,"headers": {
        "User-Agent" : "Digipay Data Scraper"
        ,"Accept" : "application/json, text/plain, */*"
        ,"Accept-Language" : "en-US,en;q=0.5"
        ,"Accept-Encoding" : "gzip, deflate, br"
        ,"Content-Type" : "application/json;charset=utf-8"
        ,"token" : "5a3b2d804b6c4dec8e014d1fc1a5682f"
        ,"Origin" : "https://dashboard.pod.ir"
        ,"Connection" : "keep-alive"
        ,"Referer" : "https://dashboard.pod.ir/"
        ,"Sec-Fetch-Dest" : "empty"
        ,"Sec-Fetch-Mode" : "cors"
        ,"Sec-Fetch-Site" : "same-site"
        ,"TE" : "trailers"
    }
    ,"data":{"filterVOS":[],"parentParams":[{"fill":"BY_BUSINESS_OR_PARENT","id":785,"key":"START_DATE","order":0,"type":"DATE_FROM","validValueType":"NONE","value":"30"},{"fill":"BY_BUSINESS_OR_PARENT","id":786,"key":"END_DATE","order":1,"type":"DATE_FROM","validValueType":"NONE","value":"0"}],"orderByElementVOS":[]}
    ,"oauth":{
         "client_id":"9326820e9ba14d9b822f51010028cf00"
        ,"token_url":"https://accounts.pod.ir/oauth2/token"
        ,"refresh_token":"2ba09737435f4a0eb0cbfce380d90573"
        ,"grant_type":"refresh_token"
        ,"code_verifier":"1KbYzrQBANWOk2tQIezpWHzEPty0vir1Z_DrmYjYVr0"
    }}
try:
    with open("pod_client.json","r") as fp:
        pod_client=json.load(fp)
        fp.close()
except Exception as ex:
    print(ex)


resp = requests.post(pod_client["url"], headers=pod_client["headers"], data=json.dumps(pod_client["data"]))
if resp.status_code==401:
    oauth = pod_client["oauth"]
    oauth_session = OAuth2Session(client_id=oauth["client_id"])
    token = oauth_session.refresh_token(token_url=oauth["token_url"],refresh_token=oauth["refresh_token"]
        ,body="grant_type=%s&client_id=%s&refresh_token=%s&code_verifier=%s"%(oauth["grant_type"],oauth["client_id"],oauth["refresh_token"],oauth["code_verifier"])
        )
    pod_client["headers"]["token"] = token["access_token"]
    with open("pod_client.json","w") as fp:
        json.dump(pod_client,fp)
        fp.close()
    resp = requests.post(pod_client["url"], headers=pod_client["headers"], data=pod_client["data"])

print(resp.status_code)
print(resp.text)
