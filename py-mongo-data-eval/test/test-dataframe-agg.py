import json

import jdatetime
import yaml

import pandas as pd
import numpy as np

# df = pd.DataFrame({'14001010':     ['090001',0, None],
#                        '14001011': ['090002',0, 0],
#                        '14001012': ['090003',1, 0],
#                        '14001013': ['090004',0, 1],
#                        '14001014': ['090005',1, 1],
#                        '14001015': ['090006',None, 1]
#                    })
df = pd.DataFrame({    '14001010': [0, None],
                       '14001011': [0, 0],
                       '14001012': [1, 0],
                       '14001013': [0, 1],
                       '14001014': [1, 1],
                       '14001015': [None, 1]
                   })
print('df=',df)

startdate=jdatetime.datetime(1400,10,10)
enddate=jdatetime.datetime(1400,10,15)
delta = enddate - startdate
for i in range(delta.days+1):
    jdate = startdate+jdatetime.timedelta(days=i)
    jdate_str = jdate.strftime('%Y%m%d')
    print('AGG=>',jdate_str,df.agg({jdate_str:['sum']})[jdate_str]['sum'])
    if i>0 :
        jdate_prevday_str=(jdate-jdatetime.timedelta(days=1)).strftime('%Y%m%d')
        _groupby = df.groupby([jdate_str,jdate_prevday_str]).size()
        # if (1,1) in _groupby:
            # print('GROUPBY=>',jdate_str,_groupby[1][1])


