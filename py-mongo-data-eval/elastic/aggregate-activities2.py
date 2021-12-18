import requests
import pprint

sql_ipg_active_users_count_before_jalali_date={'_source': False,
'aggregations': {'groupby': {'aggregations': {'active_users_count': {'cardinality': {'field': 'owner_debtor_username.keyword'}}},
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
                                                                 'to': 13990730 }}},
                                      {'terms': {'boost': 1.0,
                                                 'gateway': [0,1]}}]}},
                           {'terms': {'boost': 1.0,
                              'type': [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}}]}},
'size': 0,
'stored_fields': '_none_'}

count_tnx_start_to_jalali_date={ "query": {"bool": {
    'must': [{'range': {'jalali_date': {'boost': 1.0,
                                        'from': None,
                                        'include_lower': False,
                                        'include_upper': True,
                                        'to': 13990830 }}}]
}}}

ES_INDEX="activities2"
# ES_QUERY=total_by_jalali_year
# ES_QUERY=total_by_jalali_month
# ES_QUERY=sql_ipg_active_users_count_before_jalali_date
ES_QUERY=count_tnx_start_to_jalali_date

# resp = requests.post('http://localhost:9200/'+ES_INDEX+'/_search?size=0',json=ES_QUERY)
resp = requests.post('http://localhost:9200/'+ES_INDEX+'/_count',json=ES_QUERY)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

#1399/07/30
# active user = 5,203,582
# tnx count = 26,303,658
