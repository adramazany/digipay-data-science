import requests
import pprint
import datetime
import jdatetime
import time
import sys
import traceback
import logging
from digipay import dateutil
import cx_Oracle

query_activities_day_stat_by_date={'_source': {'excludes': [],
                                       'includes': ['jalali_date', 'count', 'amount', 'feeCharge']},
                           'docvalue_fields': [{'field': 'date'}],
                           'query': {'term': {'jalali_date.keyword': {'boost': 1.0, 'value': '13990909'}}},
                           'size': 1000,
                           'sort': [{'_doc': {'order': 'asc'}}]}

query_activities_aggs_by_jalali_date={'_source': False,
                                      'aggregations': {'groupby': {'aggregations': {'amount': {'sum': {'field': 'amount'}},
                                                                                    'feeCharge': {'sum': {'field': 'feeCharge'}}},
                                                                   'composite': {'size': 1000,
                                                                                 'sources': [{'groupby': {'terms': {'field': 'jalali_date.keyword',
                                                                                                                     'missing_bucket': True,
                                                                                                                     'order': 'asc'}}}]}}},
                                      'query': {'term': {'jalali_date.keyword': {'boost': 1.0, 'value': 13980201}}},
                                      'size': 0,
                                      'stored_fields': '_none_'}

query_oracle_ft_activities_jalali_date='select f_pdate,count(*) count,sum(amount) amount,sum(feecharge) feecharge from ft_activities where f_pdate=:f_pdate group by f_pdate'

class AGG_RESULT:
    count=0
    amount=0
    feeCharge=0

def contradiction_activities_with_day_stat(server, src_index, dest_index, date_jalali):
    query_activities_aggs_by_jalali_date['query']['term']['jalali_date.keyword']['value']=date_jalali.strftime("%Y%m%d")
    logging.debug(query_activities_aggs_by_jalali_date)
    src_resp = requests.get(server +'/' + src_index + '/_search', json=query_activities_aggs_by_jalali_date)
    if src_resp.status_code != 200:
        logging.error(src_resp)
        raise Exception('GET /tasks/ {}'.format(src_resp.status_code))
    logging.debug(src_resp.json())
    json=src_resp.json()
    src=AGG_RESULT()
    if len(json['hits']['hits'])>0:
        src.count=json['hits']['hits'][0]['_source']['count']
        src.amount=json['hits']['hits'][0]['_source']['amount']
        src.feeCharge=json['hits']['hits'][0]['_source']['feeCharge']
    logging.debug("SRC %s  count=%d , amount=%d , feeCharge=%d"%(date_jalali.strftime("%Y%m%d"),src.count,src.amount,src.feeCharge))

    query_activities_day_stat_by_date['query']['term']['jalali_date.keyword']['value']=date_jalali.togregorian().strftime("%Y-%m-%d")
    logging.debug(query_activities_day_stat_by_date)
    dest_resp = requests.get(server +'/' + dest_index + '/_search', json=query_activities_day_stat_by_date)
    if dest_resp.status_code != 200:
        logging.error(dest_resp)
        raise Exception('GET /tasks/ {}'.format(dest_resp.status_code))
    logging.debug(dest_resp.json())
    json=dest_resp.json()
    dest = AGG_RESULT
    if len(json['hits']['hits'])>0:
        dest.count=json['hits']['hits'][0]['_source']['count']
        dest.amount=json['hits']['hits'][0]['_source']['amount']
        dest.feeCharge=json['hits']['hits'][0]['_source']['feeCharge']
    logging.debug("DEST %s  count=%d , amount=%d , feeCharge=%d"%(date_jalali.strftime("%Y%m%d"),dest.count,dest.amount,dest.feeCharge))

    if src.count !=  dest.count or src.amount !=  dest.amount or src.feeCharge!=  dest.feeCharge :
        logging.error("contradiction detected at :%s => SRC count=%d , amount=%d , feeCharge=%d <> DEST count=%d , amount=%d , feeCharge=%d"%(date_jalali.strftime("%Y%m%d"),src.count,src.amount,src.feeCharge,dest.count,dest.amount,dest.feeCharge))
        return False
    else:
        return True


ES_SERVER="http://localhost:9200"
ES_SRC_INDEX= "activities2"
ES_DEST_INDEX="activities_day_stat"
# ES_DEST_INDEX="activities_day_stat_991008"
MAX_RETRY_COUNT=1
# logging.basicConfig(level=logging.DEBUG)

# JALAL_DATE=13990631
start_date_jalali=jdatetime.datetime(1397,7,26)
end_date_jalali=jdatetime.datetime.now()

total_count=0
error_count=0
retry_count=0

t1= int(round(time.time() * 1000))

while start_date_jalali<end_date_jalali:
    try:
        # print("etl starting:",start_date_jalali)

        if  contradiction_activities_with_day_stat(ES_SERVER,ES_SRC_INDEX,ES_DEST_INDEX,start_date_jalali)==False :
            error_count+=1
        total_count+=1

        print("contradiction end:",start_date_jalali," , TOTAL COUNTER=",total_count," , ERROR =",error_count," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
        start_date_jalali=(start_date_jalali+datetime.timedelta(days=1))

    #raise ValueError
    except Exception as err:
        print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
        logging.error(traceback.format_exc())
        print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
        print("total count till now=",total_count)
        print("total error till now=",error_count)
        retry_count+=1
        if retry_count>MAX_RETRY_COUNT : raise Exception("retry","retry count exceeded!")

print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")

