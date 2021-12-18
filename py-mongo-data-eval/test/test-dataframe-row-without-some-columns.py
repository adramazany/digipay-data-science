import pandas as pd
import numpy as np

df = pd.DataFrame({'a': ['1', '2'],
                       'b': ['45.8', '73.9'],
                       'c': [10.5, 3.7],
                       'd': ['ali', 12],
                       'e': ['hasan', 34.44],
                       'e1': [None, 34.44],
                       'e2': ['ali',None],
                       'f': ['test1', 'test2']
                   })

print('df=',df)
print('df.columns=',df.columns)
print('df.values=',df.values)

df_without = df[ df.columns.difference(["e","e1","e2"]) ]
for i, row in df_without.iterrows():
# for row in df.values.tolist():
#     print("df_without row=",(",").join( row.values ) )
    print("df_without row=", row.values  )
    print("row",df.values[i])
    print("row.a",df['a'][i])

