# POD get report
import requests
from requests.structures import CaseInsensitiveDict

url = "https://dashboardservice.pod.ir/v1/userreport/1414/getCSV?forceSave=false"

headers = CaseInsensitiveDict()
headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0"
headers["Accept"] = "application/json, text/plain, */*"
headers["Accept-Language"] = "en-US,en;q=0.5"
headers["Accept-Encoding"] = "gzip, deflate, br"
headers["Content-Type"] = "application/json;charset=utf-8"
headers["token"] = "f69e04006b854641b5e53a47ed2c64b8"
headers["Origin"] = "https://dashboard.pod.ir"
headers["Connection"] = "keep-alive"
headers["Referer"] = "https://dashboard.pod.ir/"
headers["Sec-Fetch-Dest"] = "empty"
headers["Sec-Fetch-Mode"] = "cors"
headers["Sec-Fetch-Site"] = "same-site"
headers["TE"] = "trailers"

data = '{"filterVOS":[],"parentParams":[{"fill":"BY_BUSINESS_OR_PARENT","id":785,"key":"START_DATE","order":0,"type":"DATE_FROM","validValueType":"NONE","value":"30"},{"fill":"BY_BUSINESS_OR_PARENT","id":786,"key":"END_DATE","order":1,"type":"DATE_FROM","validValueType":"NONE","value":"0"}],"orderByElementVOS":[]}'

resp = requests.post(url, headers=headers, data=data)

print(resp.status_code)
print(resp.text)

