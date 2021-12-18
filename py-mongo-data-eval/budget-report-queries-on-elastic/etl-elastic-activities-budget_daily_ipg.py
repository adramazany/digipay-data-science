import requests
import pprint
import datetime
import jdatetime
import time
import sys
import traceback
import logging
from digipay import dateutil
from digipay import config



sql_ipg_active_users_count_before_jalali_date={'_source': False,
                                               'aggregations': {'groupby': {'aggregations': {'active_users_count': {'cardinality': {'field': 'owner_debtor_cellNumber.keyword'}}},
                                                                            'filters': {'filters': [{'match_all': {'boost': 1.0}}],
                                                                                        'other_bucket': False,
                                                                                        'other_bucket_key': '_other_'}}},
                                               'query': {'bool': {'adjust_pure_negative': True,
                                                                  'boost': 1.0,
                                                                  'must': [{'bool': {'adjust_pure_negative': True,
                                                                                     'boost': 1.0,
                                                                                     'must': [{'range': {'jalali_date': {'boost': 1.0,
                                                                                                                         'from': None,
                                                                                                                         'include_lower': False,
                                                                                                                         'include_upper': True,
                                                                                                                         'to': 13980101 }}},
                                                                                              {'terms': {'boost': 1.0,
                                                                                                         'gateway': [0,1]}}]}},
                                                                           {'terms': {'boost': 1.0,
                                                                                      'type': [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}}]}},
                                               'size': 0,
                                               'stored_fields': '_none_'}


sql_ipg_customers_count_before_jalali_date={'_source': False,
                                            'aggregations': {'groupby': {'aggregations': {'customers_count': {'cardinality': {'field': 'owner_debtor_cellNumber.keyword'}}},
                                                                         'filters': {'filters': [{'match_all': {'boost': 1.0}}],
                                                                                     'other_bucket': False,
                                                                                     'other_bucket_key': '_other_'}}},
                                            'query': {'bool': {'adjust_pure_negative': True,
                                                               'boost': 1.0,
                                                               'must': [{'bool': {'adjust_pure_negative': True,
                                                                                  'boost': 1.0,
                                                                                  'must': [{'bool': {'adjust_pure_negative': True,
                                                                                                     'boost': 1.0,
                                                                                                     'must': [{'range': {'jalali_date': {'boost': 1.0,
                                                                                                                                         'from': None,
                                                                                                                                         'include_lower': False,
                                                                                                                                         'include_upper': True,
                                                                                                                                         'to': 13980101}}},
                                                                                                              {'term': {'status': {'boost': 1.0,'value': 0}}}]}},
                                                                                           {'terms': {'boost': 1.0,'gateway': [0,1]}}]}},
                                                                        {'terms': {'boost': 1.0,
                                                                                   'type': [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}}]}},
                                            'size': 0,
                                            'stored_fields': '_none_'}

url_budget_daily_ipg_prev_day="{0}/budget_daily_ipg/_doc/{1}?_source_includes={2}"

def elastic_execute_query(query,index):
    resp = requests.post('http://localhost:9200/'+index+'/_search',json=query,auth=config.auth)
    if resp.status_code != 200:
        print("resp=",resp.content)
        raise Exception('POST {}'.format(resp.status_code))
    # print("elastic_execute_query",resp.json())
    return resp.json()

def get_budget_daily_feild(server,index,id,fields):
    url = "http://{0}/{1}/_doc/{2}?_source_includes={3}".format(server,index,id,",".join(fields))
    # print(url)
    resp = requests.get(url,auth=config.auth)
    if resp.status_code != 200:
        print("get_budget_daily_feild not found for =",resp.content)
        # raise Exception('POST {}'.format(resp.status_code))
        res={}
        for f in fields:res[f]=0
        return res
    # print("get_budget_daily_feild",resp.json()["_source"][field],resp.json())
    return resp.json()["_source"]

def get_customer_count(from_date_jalali,to_date_jalali):
    _from = (None if from_date_jalali  is None else str(jdatetime.date.strftime(from_date_jalali,'%Y%m%d')))
    _to   = (None if to_date_jalali  is None else str(jdatetime.date.strftime(to_date_jalali,'%Y%m%d')))
    sql_ipg_customers_count_before_jalali_date['query']['bool']['must'][0]['bool']['must'][0]['bool']['must'][0]['range']['jalali_date']['from']    =_from
    sql_ipg_customers_count_before_jalali_date['query']['bool']['must'][0]['bool']['must'][0]['bool']['must'][0]['range']['jalali_date']['to']      =_to
    # print("sql_ipg_customers_count_before_jalali_date=",sql_ipg_customers_count_before_jalali_date)
    res = elastic_execute_query(sql_ipg_customers_count_before_jalali_date, ES_QUERY_INDEX)['aggregations']['groupby']['buckets'][0]['customers_count']['value']
    # print("get_customer_count({0} , {1}) = {2}".format(_from,_to,res))
    return res


def calc_and_save_budget_daily(server,budget_daily_index,date_jalali):
    date_jalali_str=str(jdatetime.date.strftime(start_date_jalali,'%Y%m%d'))
    row={
        "date":datetime.date.strftime(date_jalali.togregorian(),"%Y-%m-%d")
        ,"jalali_date":date_jalali_str
        ,"jalali_year":start_date_jalali.year
        ,"jalali_month":start_date_jalali.month
        ,"jalali_day":start_date_jalali.day
        # ,"digikala_transactions":0
        # ,"digikala_revenue":0
        # ,"fidibo_transactions":0
        # ,"fidibo_revenue":0
        # ,"others_transactions":0
        # ,"others_revenue":0
        # ,"ipg_purchase_transactions_amount":0
        # ,"total_expected_revenue":0
        # ,"total_real_revenue_monthly":0
        # ,"total_realized_revenue_monthly":0
        # ,"reconcilliation_result":0
    }

    ######### prev_budget_daily
    prev_date_jalali = str(jdatetime.datetime.strftime((date_jalali-jdatetime.timedelta(days=1)),'%Y%m%d'))
    prev_budget_daily = get_budget_daily_feild(server,budget_daily_index,prev_date_jalali,["active_user","customer"])

    ######### active_user
    sql_ipg_active_users_count_before_jalali_date['query']['bool']['must'][0]['bool']['must'][0]['range']['jalali_date']['to']=date_jalali_str
    row["active_user"] = elastic_execute_query(sql_ipg_active_users_count_before_jalali_date, ES_QUERY_INDEX)['aggregations']['groupby']['buckets'][0]['active_users_count']['value']

    ######### new_active_user
    row["new_active_user"] = row["active_user"] - prev_budget_daily["active_user"]

    ######### customer
    # sql_ipg_customers_count_before_jalali_date['query']['bool']['must'][0]['bool']['must'][0]['bool']['must'][0]['range']['jalali_date']['to']=date_jalali_str
    # row["customer"] = elastic_execute_query(sql_ipg_customers_count_before_jalali_date, ES_QUERY_INDEX)['aggregations']['groupby']['buckets'][0]['customers_count']['value']
    # print ("customer",row["customer"])
    row["customer"] = get_customer_count( None , date_jalali )
    # print ("customer",row["customer"])

    ######### new_customer
    row["new_customer"] = row["customer"] - prev_budget_daily["customer"]

    ######### active_customer_1m
    row["active_customer_1m"] = get_customer_count( dateutil.prev_month_jalali(date_jalali,1) , date_jalali  )

    ######### active_customer_2m
    row["active_customer_2m"] = get_customer_count( dateutil.prev_month_jalali(date_jalali,2) , date_jalali  )

    ######### active_customer_3m
    row["active_customer_3m"] = get_customer_count( dateutil.prev_month_jalali(date_jalali,3) , date_jalali  )

    ######### active_customer_6m
    row["active_customer_6m"] = get_customer_count( dateutil.prev_month_jalali(date_jalali,6) , date_jalali  )

    ######### active_customer_12m
    row["active_customer_12m"] = get_customer_count( dateutil.prev_month_jalali(date_jalali,12) ,  date_jalali )

    print("budget_daily_ipg count before ({0}) = {1}".format( date_jalali_str , row  ))

    ###############  save budget_daily in elasticsearch
    resp = requests.post('http://localhost:9200/'+ES_BUDGET_DAILY_INDEX+'/_doc/'+str(date_jalali_str),json=row,auth=config.auth)
    if resp.status_code > 300:
        print("resp=",resp.content)
        raise Exception('POST {}'.format(resp.status_code))
    # pprint.pprint(resp.json())


ES_SERVER="localhost:9200"
ES_QUERY_INDEX= "activities2"
ES_BUDGET_DAILY_INDEX="budget_daily_ipg_cell"
# ES_QUERY=sql_ipg_active_users_count_before_jalali_date
MAX_RETRY_COUNT=3

# JALAL_DATE=13990631
start_date_jalali=jdatetime.datetime(1397,7,26)
end_date_jalali=jdatetime.datetime(1399,10,6)

total_count=0
retry_count=0

t1= int(round(time.time() * 1000))

while start_date_jalali<end_date_jalali:
    try:
        print("etl starting:",start_date_jalali)

        calc_and_save_budget_daily(ES_SERVER,ES_BUDGET_DAILY_INDEX,start_date_jalali)
        total_count+=1

        print("etl end:",start_date_jalali," , COUNTER=",total_count," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
        start_date_jalali=(start_date_jalali+datetime.timedelta(days=1))

    #raise ValueError
    except Exception as err:
        print("Unexpected error:", sys.exc_info()[0]," on date:",start_date_jalali)
        logging.error(traceback.format_exc())
        print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
        print("total count till now=",total_count)
        retry_count+=1
        if retry_count>MAX_RETRY_COUNT : raise Exception("retry","retry count exceeded!")

print("end-time=",datetime.datetime.now()," , DURATION=",(int(round(time.time() * 1000))-t1),"(ms)")
