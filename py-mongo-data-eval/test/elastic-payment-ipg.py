import requests
import pprint

JALAL_DATE=990631

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
                                                                                                      'to': JALAL_DATE }}},
                                                                           {'terms': {'boost': 1.0,
                                                                                      'gateway': [0,1]}}]}},
                                                        {'terms': {'boost': 1.0,
                                                                   'type': [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}}]}},
                            'size': 0,
                            'stored_fields': '_none_'}


sql_ipg_customers_count_before_jalali_date={'_source': False,
 'aggregations': {'groupby': {'aggregations': {'customers_count': {'cardinality': {'field': 'owner_debtor_username.keyword'}}},
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
                                                                                              'to': JALAL_DATE}}},
                                                                   {'term': {'status': {'boost': 1.0,'value': 0}}}]}},
                                                {'terms': {'boost': 1.0,'gateway': [0,1]}}]}},
                             {'terms': {'boost': 1.0,
                                        'type': [0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170]}}]}},
 'size': 0,
 'stored_fields': '_none_'}

def elastic_execute_query(query,index):
    resp = requests.post('http://localhost:9200/'+index+'/_search',json=query)
    if resp.status_code != 200:
        print("resp=",resp.content)
        raise Exception('POST {}'.format(resp.status_code))
    print("elastic_execute_query",resp.json())
    return resp.json()


ES_INDEX="activities2"
# ES_QUERY=sql_ipg_active_users_count_before_jalali_date
ES_QUERY=sql_ipg_customers_count_before_jalali_date

active_users_count = elastic_execute_query(sql_ipg_active_users_count_before_jalali_date,ES_INDEX)['aggregations']['groupby']['buckets'][0]['active_users_count']['value']
customers_count = elastic_execute_query(sql_ipg_customers_count_before_jalali_date,ES_INDEX)['aggregations']['groupby']['buckets'][0]['customers_count']['value']

print("ipg active users count before ({0}) = {1:,}".format( JALAL_DATE , active_users_count  ))
print("ipg customers count before ({0}) = {1:,}".format( JALAL_DATE , customers_count  ))
# 5,658,528
# 4,360,417
