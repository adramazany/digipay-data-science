import requests
from requests.auth import HTTPBasicAuth
import pprint
from digipay import config

sql_select_order_limit={
    "query": "SELECT * FROM activities_test ORDER BY datetime DESC",
    "fetch_size": 10
}
sql_select_count_distinct={
    "query": "SELECT count(distinct owner_debtor_username) FROM activities_test ",
    "fetch_size": 10
}
sql_ipg_active_users_lifetime_in_jalali_date={
    "query": "SELECT count(distinct owner_debtor_username) FROM activities_test "
             " where jalali_date<=990131 "
             " and gateway in (0,1) "
             " and type in (0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170) "
}
sql_ipg_customers_lifetime_in_jalali_date={
    "query": "SELECT count(distinct owner_debtor_username) FROM activities_test "
             " where jalali_date<=990131 "
             " and status=0 "
             " and gateway in (0,1) "
             " and type in (0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170) "
}

sql_budget_monthly_ipg1={
    "query":"select left(jalali_date,6) jalali_month"
            ",max(active_user) active_user"
            ",max(customer) customer"
            ",(max(active_user)-(select max(active_user) from budget_daily_ipg prev"
            "    where left(prev.jalali_date,6)=(left(d.jalali_date,6)-1)"
            ")) new_active_user"
            ",(max(customer)-(select max(customer) from budget_daily_ipg prev"
            "where left(prev.jalali_date,6)=(left(d.jalali_date,6)-1)"
            ")) new_customer"
            ",left(jalali_date,6)-1 prev_month"
            "from budget_daily_ipg d"
            "group by left(jalali_date,6)"
}

sql_budget_monthly_ipg={
    "query":"select substr(d.jalali_date,1,6) jalali_month "
    " ,max(d.active_user) active_user "
    " ,max(d.customer) customer "
    " ,(max(d.active_user)-p.active_user) new_active_user "
    " ,(max(d.active_user)-p.active_user) new_customer "
    " ,substr(jalali_date,1,6)-1 prev_month "
    " from budget_daily_ipg d "
    " left join (select substr(jalali_date,1,6) jalali_month,max(active_user) active_user,max(customer) customer "
    " from budget_daily_ipg group by substr(jalali_date,1,6) ) p "
    " on cast(p.jalali_month as int)=(cast(substr(d.jalali_date,1,6) as int)-1) "
    " group by substr(d.jalali_date,1,6) "
}

sql_activities_day_stat_by_jalali_day={
    "query":"select date,jalali_date_time,count,amount,feeCharge from activities_day_stat where date='2019-05-19'"
}

sql_activities_aggs_by_jalali_date={
    "query":"select jalali_date,count(*) count,sum(amount) amount,sum(feeCharge) feeCharge from activities2 where jalali_date=13980201 group by jalali_date"
}

sql_activities_null_src_username_count={
    "query":"select count(*) count from activities2 where src_owner_username is null"
}
# 324,070
sql_activities_null_src_cellNumber_count={
    "query":"select count(*) count from activities2 where src_owner_cellNumber is null"
}
# 324,070
sql_agg_app_distinct_user_tnx_count_lifetime={
    "query":"select src_owner_cellNumber,count(*) count from activities2 where is_app=1 and jalali_date<13991001 group by src_owner_cellNumber order by count(*) desc"
}


# ES_QUERY=sql_select_order_limit
# ES_QUERY=sql_select_count_distinct
# ES_QUERY=sql_ipg_active_users_lifetime_in_jalali_date
# ES_QUERY=sql_ipg_customers_lifetime_in_jalali_date
# ES_QUERY=sql_budget_monthly_ipg
# ES_QUERY=sql_activities_day_stat_by_jalali_day
# ES_QUERY=sql_activities_aggs_by_jalali_date
# ES_QUERY=sql_activities_null_src_username_count
ES_QUERY=sql_agg_app_distinct_user_tnx_count_lifetime


resp = requests.post("http://localhost:9200/_sql/translate",json=ES_QUERY,auth=config.auth)
if resp.status_code != 200:
    #print("resp=",resp.content)
    pprint.pprint(resp.json())
    raise Exception('POST {}'.format(resp.status_code))

query = resp.text.replace("'","\"").replace("False","false").replace("True","true")
pprint.pprint(resp.json())
print("final query=",query)
