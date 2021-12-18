import requests
import pprint

query={
  "query": {
      "bool": {
          "must_not": [
              {"exists":{"field": "datetime5"}}
          ]
      }}
    ,"script": {
        "source": "ctx._source.datetime2=new Date((long)(ctx._source.creationDate+12600000));"
        ,"lang": "painless"
    }
}
# 'Calendar cal=Calendar.getInstance(TimeZone.getTimeZone("Asia/Tehra"));'
# 'cal.setTimeInMillis((long)ctx._source.creationDate);'
# 'java.sql.Timestamp ts = new java.sql.Timestamp((long)ctx._source.creationDate);'
# 'ctx._source.datetime4=new java.util.Date(ts.getTime());'
# 'ctx._source.datetime4=cal.getTime();'
# "ctx._source.datetime2=new Date((long)ctx._source.creationDate);"

resp = requests.post('http://localhost:9200/activities_test2/_update_by_query?scroll_size=10000',json=query)
if resp.status_code != 200:
    #print("resp=",resp.content)
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())
