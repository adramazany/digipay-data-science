import json
import yaml

import pandas as pd
import numpy as np

df = pd.DataFrame({'a': ['1', '2'],
                       'b': ['45.8', '73.9'],
                       'c': [10.5, 3.7],
                       'd': ['ali', 12],
                       'e': ['hasan', 34.44],
                       'e1': [None, 34.44],
                       'e2': ['ali',None],
                       'json_field': ['[{"name":"ali","family":"ram","age":"18","f11":"v11"},{"name":"mohammad","family":"ram","age":"28","f12":"v12"}]',
                                 '[{"name":"hassan","family":"ahmadi","age":"118","f21":"v21"},{"name":"reza","family":"ahmadi","age":"128","f22":"v22"}]'],
                       'f': ['test1', 'test2']
                   })

print('df=',df)
print('df=',df['a']) # just column
print('df=',df[['a']]) # column with index

