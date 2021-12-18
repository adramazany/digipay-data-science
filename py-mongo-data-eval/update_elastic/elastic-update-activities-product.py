import requests
import pprint

query={
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
      }}
    ,"script": {
        "source": "List app_types=[12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 140, 170];"
                  "List topup_types=[30,31];"
                  "List bill_types=[40];"
                  "List ipg_gateways=[0,1];"
                  "List ipg_types=[0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];"
                  "List wallet_types=[15, 16, 17];"
                  "List wallet_gateway4_types=[0, 1, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];"
                  "if(app_types.contains(ctx._source.type)){ ctx._source.is_app = 1;}"
                  +"if(topup_types.contains(ctx._source.type)){ ctx._source.is_topup = 1;}"
                  +"if(bill_types.contains(ctx._source.type)){ ctx._source.is_bill = 1;}"
                  +"if(ipg_gateways.contains(ctx._source.gateway) && !ctx._source['owner_debtor_cellNumber'].empty && ipg_types.contains(ctx._source.type)){ ctx._source.is_ipg = 1;}"
                  +"if( (wallet_types.contains(ctx._source.type)) || (ctx._source.gateway==4 && wallet_gateway4_types.contains(ctx._source.type) ) ){ ctx._source.is_wallet = 1;}"
        ,"lang": "painless"
    }
}

resp = requests.post('http://localhost:9200/activities/_update_by_query?scroll_size=10000',json=query)
if resp.status_code != 200:
    #print("resp=",resp.content)
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())
