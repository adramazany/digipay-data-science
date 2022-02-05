import json
import math

import jdatetime
import yaml

import pandas as pd
import numpy as np

df = pd.DataFrame({    '140001': [1048575, 1048575], # first 20 complete
                       '140002': [33554431, 33554431], # first 25 days
                       '140003': [2147483647 , 2147483647 ], # first 31 days
                       '140004': [31, 31], # first 5 days
                       '140005': [1023, 1023], #first 10 days
                       '140006': [32767, 32767] # first 15 days
                   })
print('df=',df)

startdate=jdatetime.datetime(1400,5,1)
enddate=jdatetime.datetime(1400,6,30)
delta = enddate - startdate
for i in range(delta.days+1):
    jdate = startdate+jdatetime.timedelta(days=i)
    jdate_str = jdate.strftime('%Y%m%d')
    jmonth_str = jdate.strftime('%Y%m')
    jday = jdate.day
    p_jday=2**(jday-1)
    print(jmonth_str,jday)
    print('AGG=>',jdate_str,df.where(df[jmonth_str]&p_jday==p_jday).agg({jmonth_str:['count']})[jmonth_str]['count'])
    if i>0 :
        jdate_prevday_str=(jdate-jdatetime.timedelta(days=1)).strftime('%Y%m%d')
        jprevmonth_str=(jdate-jdatetime.timedelta(days=31)).strftime('%Y%m')
        jprevmonth_day=(jdate-jdatetime.timedelta(days=31)).day
        p_jprevmonth_day=2**(jprevmonth_day-1)
        _groupby = df.where( (df[jmonth_str]&p_jday==p_jday)
                             & (df[jprevmonth_str]&p_jprevmonth_day==p_jprevmonth_day)) \
                             .agg({jmonth_str:['count']})[jmonth_str]['count']
                             # .groupby([jmonth_str,jprevmonth_str]).size()
        print('_PREVMONTH=',jdate_str,_groupby)
        # if len(_groupby)>0:
        #     print('_groupby=',len(_groupby),_groupby)
        # if (1,1) in _groupby:
        #     print('GROUPBY=>',jdate_str,_groupby[1][1])


# b=0
# c=0
# for i in range(0,32):
#     p=2**i
#     b+= p
#     c = c | p
#     print(i+1,b,c, (b & p)==p )
    # if i>1 :
    #     print(isPowerOf(b,i))
#
# 1 1 1 True
# 2 3 3 True
# 3 7 7 True
# 4 15 15 True
# 5 31 31 True
# 6 63 63 True
# 7 127 127 True
# 8 255 255 True
# 9 511 511 True
# 10 1023 1023 True
# 11 2047 2047 True
# 12 4095 4095 True
# 13 8191 8191 True
# 14 16383 16383 True
# 15 32767 32767 True
# 16 65535 65535 True
# 17 131071 131071 True
# 18 262143 262143 True
# 19 524287 524287 True
# 20 1048575 1048575 True
# 21 2097151 2097151 True
# 22 4194303 4194303 True
# 23 8388607 8388607 True
# 24 16777215 16777215 True
# 25 33554431 33554431 True
# 26 67108863 67108863 True
# 27 134217727 134217727 True
# 28 268435455 268435455 True
# 29 536870911 536870911 True
# 30 1073741823 1073741823 True
# 31 2147483647 2147483647 True