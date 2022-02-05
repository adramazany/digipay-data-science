import json
import yaml

import pandas as pd
import numpy as np

df = pd.DataFrame({'a': ['09120001','1', '2'],
                       'b': [9120002,'45.8', '73.9'],
                       'c': [9120003,10.5, 3.7],
                       'd': [9120004,'ali', 12],
                       'e': [9120005,'hasan', 34.44],
                       'e1': [9120006,None, 34.44],
                       'e2': [9120007,'ali',None],
                       'json_field': [9120008,'[{"name":"ali","family":"ram","age":"18","f11":"v11"},{"name":"mohammad","family":"ram","age":"28","f12":"v12"}]',
                                 '[{"name":"hassan","family":"ahmadi","age":"118","f21":"v21"},{"name":"reza","family":"ahmadi","age":"128","f22":"v22"}]'],
                       'f': [9120009,'test1', 'test2']
                   })

df.set_index(keys=['a'],inplace=True)

print('df=',df)

# v1 = df.iloc[1]
v1 = df.loc['09120001']
# v1 = df.loc['b':9120000]
# v1['b']=100.001 # See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
df.set_value('09120001','b',100000)

print('df.iloc=',v1)

print('df[b]=',df['b']) # just column
print('df=',df[['b']]) # column with index


