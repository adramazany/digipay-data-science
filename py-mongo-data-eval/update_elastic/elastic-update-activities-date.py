
import datetime
import jdatetime
from elasticsearch import Elasticsearch,helpers

counter=0
es = Elasticsearch()
for document in helpers.scan(es, index="activities"):
    timestamp=float(document["_source"]["creationDate"])
    date_time=datetime.datetime.fromtimestamp(timestamp/1000)
    jalali_date=jdatetime.date.fromtimestamp(timestamp/1000)
    jalali_date_str= str(jdatetime.datetime.strftime(jalali_date,'%Y%m%d'))
    counter+=1
    if counter%1000==0 : print("counter:",counter)
    es.update(index=document['_index'], doc_type=document['_type'], id=document['_id'], body={
        "script" : {
            "source": "ctx._source.datetime=params.datetime;"
                      "ctx._source.year=params.year;"
                      "ctx._source.month=params.month;"
                      "ctx._source.day=params.day;"
                      "ctx._source.hour=params.hour;"
                      "ctx._source.minute=params.minute;"
                      "ctx._source.second=params.second;"
                      "ctx._source.jalali_date=params.jalali_date;"
                      "ctx._source.jalali_year=params.jalali_year;"
                      "ctx._source.jalali_month=params.jalali_month;"
                      "ctx._source.jalali_day=params.jalali_day;",
            "lang": "painless",
            "params" : {
                 "datetime": date_time
                ,"year": date_time.year
                ,"month": date_time.month
                ,"day": date_time.day
                ,"hour": date_time.hour
                ,"minute": date_time.minute
                ,"second": date_time.second
                ,"jalali_date": jalali_date_str
                ,"jalali_year": jalali_date.year
                ,"jalali_month": jalali_date.month
                ,"jalali_day": jalali_date.day
    }
        }
    })


    #
    # String milliSinceEpochString = "434931330000";
    # long milliSinceEpoch = Long.parseLong(milliSinceEpochString);
    # Instant instant = Instant.ofEpochMilli(milliSinceEpoch);
    # ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of('Z'));
    # String datetime = zdt.format(DateTimeFormatter.ISO_INSTANT);