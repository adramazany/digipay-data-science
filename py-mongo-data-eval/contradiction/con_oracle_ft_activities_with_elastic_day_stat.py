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
                                   'query': {'term': {'date': {'boost': 1.0, 'value': '2019-05-19'}}},
                                   'size': 1000,
                                   'sort': [{'_doc': {'order': 'asc'}}]}

query_oracle_ft_activities_jalali_date='select f_pdate,count(*) count,sum(amount) amount,sum(feecharge) feecharge from ft_activities where f_pdate=:f_pdate group by f_pdate'

class AGG_RESULT:
    count=0
    amount=0
    feeCharge=0

def contradiction_activities_with_day_stat(oracle_cn,server, src_index, dest_index, date_jalali):
    query = query_oracle_ft_activities_jalali_date.replace(":f_pdate",date_jalali.strftime("%Y%m%d"))
    logging.debug(query)
    cursor=oracle_cn.cursor()
    cursor.execute(query)
    src_resp=cursor.fetchone()
    cursor.close()
    logging.debug(src_resp)
    src=AGG_RESULT()
    if src_resp:
        # src.count=src_resp['count']
        src.count=src_resp[1]
        # src.amount=src_resp['amount']
        src.amount=src_resp[2]
        # src.feeCharge=src_resp['feecharge']
        src.feeCharge=src_resp[3]
    logging.debug("SRC %s  count=%d , amount=%d , feeCharge=%d"%(date_jalali.strftime("%Y%m%d"),src.count,src.amount,src.feeCharge))

    query_activities_day_stat_by_date['query']['term']['date']['value']=date_jalali.togregorian().strftime("%Y-%m-%d")
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
MAX_RETRY_COUNT=1
ORACLE_DNS="172.18.24.84/orcl"
ORACLE_USER="mongodb"
ORACLE_PASS="Mongo123"
# logging.basicConfig(level=logging.DEBUG)

# JALAL_DATE=13990631
start_date_jalali=jdatetime.datetime(1398,7,26)
end_date_jalali=jdatetime.datetime(1399,9,15)

total_count=0
error_count=0
retry_count=0

t1= int(round(time.time() * 1000))

cx_Oracle.init_oracle_client("/Users/adel/ds/oracle_instantclient_19_8")
oracle_con = cx_Oracle.connect(ORACLE_USER, ORACLE_PASS, ORACLE_DNS)


while start_date_jalali<end_date_jalali:
    try:
        # print("etl starting:",start_date_jalali)

        if  contradiction_activities_with_day_stat(oracle_con,ES_SERVER,ES_SRC_INDEX,ES_DEST_INDEX,start_date_jalali)==False :
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

oracle_con.close()

print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")

