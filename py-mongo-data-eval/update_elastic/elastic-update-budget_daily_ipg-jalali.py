import requests
import pprint

query={
    "query": {
        "bool": {
            "must_not": [
                {"exists":{"field": "jalali_year1"}}
            ]
        }}
    ,"script": {
        "source": "int jalali_date=Integer.parseInt(ctx._source.jalali_date);"
                  "ctx._source.jalali_year=(int)(jalali_date/10000);"
                  "ctx._source.jalali_month=(int)(jalali_date/100)-(ctx._source.jalali_year*100);"
                  "ctx._source.jalali_yearmonth=''+ctx._source.jalali_year+(ctx._source.jalali_month<10?'/0':'/')+ctx._source.jalali_month;"
                  "ctx._source.jalali_day=jalali_date%100;"
        ,"lang": "painless"
    }
}

resp = requests.post('http://localhost:9200/budget_daily_ipg/_update_by_query?scroll_size=10000',json=query)
if resp.status_code != 200:
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())
