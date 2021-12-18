
import datetime
import jdatetime
from elasticsearch import Elasticsearch,helpers

ES_INDEX="activities_day_stat"

es = Elasticsearch()
for document in helpers.scan(es, index=ES_INDEX):
    timestamp=float(document["_source"]["oid"])*1000
    date=datetime.date.fromtimestamp(timestamp/1000)
    jalali_date=jdatetime.date.fromtimestamp(timestamp/1000)
    jalali_date_str= str(jdatetime.datetime.strftime(jalali_date,'%Y%m%d'))
    es.update(index=document['_index'], doc_type=document['_type'], id=document['_id'], body={
        "script" : {
            "source": "ctx._source.date=params.date;"
                      "ctx._source.year=params.year;"
                      "ctx._source.month=params.month;"
                      "ctx._source.day=params.day;"
                      "ctx._source.jalali_date=params.jalali_date;"
                      "ctx._source.jalali_year=params.jalali_year;"
                      "ctx._source.jalali_month=params.jalali_month;"
                      "ctx._source.jalali_day=params.jalali_day;",
            "lang": "painless",
            "params" : {
                 "date": date
                ,"year": date.year
                ,"month": date.month
                ,"day": date.day
                ,"jalali_date": jalali_date_str
                ,"jalali_year": jalali_date.year
                ,"jalali_month": jalali_date.month
                ,"jalali_day": jalali_date.day
    }
        }
    })
