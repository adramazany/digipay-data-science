""" dataframe_Split_column_dict_into_separate_columns :
    9/19/2022 11:00 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import pandas as pd

df = pd.DataFrame({'a':[1,2,3], 'b':[{'c':1}, {'d':3}, {'c':5, 'd':6,'e':{'f':7,'g':8}}]})

print(df)

# df['b'].apply(pd.Series)
df2=pd.concat([df.drop(['b'], axis=1), df['b'].apply(pd.Series)], axis=1)
print(df2)

df3 = pd.json_normalize(df['b'])
print(df3)


