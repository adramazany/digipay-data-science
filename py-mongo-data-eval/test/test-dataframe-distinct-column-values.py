import pandas as pd
import numpy as np

df = pd.DataFrame({'a': ['1', '2','1','3','1','1'],
                       'b': ['45.8', '73.9','45.8', '73.9','45.8', '73.9'],
                       'c': [10.5, 3.7,10.5, 3.7,10.5, 3.7],
                       'd': ['ali', 12,'ali', 12,'ali', 12],
                       'e': ['hasan', 34.44,'hasan', 34.44,'hasan', 34.44],
                       'e1': [None, 34.44,None, 34.44,None, 34.44],
                       'e2': ['ali',None,'ali',None,'ali',None],
                       'f': ['test1', 'test2','test1', 'test2','test1', 'test2']
                   })

print('df=',df)
print('df.columns=',df.columns)
print('df.values=',df.values)


print('df.distinct.columns=',df['a'].unique())

