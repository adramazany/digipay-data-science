""" test_num_is_between :
    4/6/2022 2:19 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import yaml

str1 = '{"HIGH":{"cnt_total_from":2001,"cnt_total_to":9999999,"cnt_per_month_from":1001,"cnt_per_month_to":9999999,"cnt_per_day_from":51,"cnt_per_day_to":999999},' \
' "MID" :{"cnt_total_from":1001,"cnt_total_to":2000,"cnt_per_month_from":201,"cnt_per_month_to":1000,"cnt_per_day_from":21,"cnt_per_day_to":50},' \
' "LOW" :{"cnt_total_from":500,"cnt_total_to":1000,"cnt_per_month_from":100,"cnt_per_month_to":200,"cnt_per_day_from":10,"cnt_per_day_to":20},}'
str = '{"HIGH":{"total":range(2001,9999999),"month":range(1001,9999999),"day":range(51,999999)},' \
' "MID" :{"total":range(1001,2000),"month":range(201,1000),"day":range(21,50)},' \
' "LOW" :{"total":range(500,1000),"month":range(100,200),"day":range(10,20)},}'
print(str)
# obj = yaml.load(str,yaml.UnsafeLoader)
obj = eval(str)
print(obj)

n=555
for l in obj.keys():
    for k in obj[l].keys():
        if n in obj[l][k]:
            print(l,k,n)
