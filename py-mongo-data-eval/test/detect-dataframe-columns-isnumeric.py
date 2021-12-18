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
print('df.dtypes=',df.dtypes)
print('df.columns=',df.columns)
print('df.values=',df.values)

# measurer = np.vectorize(len)
# df_columns_length = dict(zip(df, measurer(df.values).max(axis=0)))
# df_columns_isnumeric=df.apply(lambda s: pandas.to_numeric(s, errors='coerce').notnull().all())
# print(df_columns_isnumeric)

df_agg = df.agg(['sum','max','median'])
# df_agg = df.agg(['median'])
print('df.agg=',df_agg)

# print('df.col.isnumeric:')
# for col in df.columns:
#     print(col, df[col].str.isnumeric())

# import numbers
# >>> import decimal
# >>> [isinstance(x, numbers.Number) for x in (0, 0.0, 0j, decimal.Decimal(0))]


print( (123%1)>0 )
print( (123.0%1)>0 )
print( ((123.2%1)>0) )
print( ('a' in df and df_agg['a']['median'] and (df_agg['a']['median']%1)>0) )
print( ('f' in df and df_agg['f']['median'] and (df_agg['f']['median']%1)>0) )