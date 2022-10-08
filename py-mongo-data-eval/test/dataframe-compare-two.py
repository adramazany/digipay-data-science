import pandas as pd
import numpy as np

data_1 = {'product': ['computer','monitor','printer','desk','just_in_1',301],
          'price': [1200,800,200,350,10,511114]
          }
df1 = pd.DataFrame(data_1)
# df1.reset_index(level=[0], inplace=True)


data_2 = {'product': ['monitor','printer','desk','computer','just_in_2',301],
          'price': [800,300,350,-1,10,-1]
          }
df2 = pd.DataFrame(data_2)
# df1.reset_index(level=[0], inplace=True)

############   1      #####
# df1['price_2'] = df2['price_2']
# df1['prices_match'] = np.where(df1['price_1'] == df2['price_2'], 'True', 'False')
# df1['price_diff'] = np.where(df1['price_1'] == df2['price_2'], 0, df1['price_1'] - df2['price_2'])
# print(df1)

############   2      #####
# print("-"*5)
# print("Dataframe difference keeping equal values -- \n")
# print(df1.compare(df2, keep_equal=True))
#
# print("-"*5)
# print("Dataframe difference keeping same shape -- \n")
# print(df1.compare(df2, keep_shape=True))
#
# print("-"*5)
# print("Dataframe difference keeping same shape and equal values -- \n")
# print(df1.compare(df2, keep_shape=True, keep_equal=True))


############   3      #####
# df = pd.concat([df1,df2]).drop_duplicates().reset_index(drop=True)
# df = pd.concat([df1,df2]).drop_duplicates(keep=False).reset_index(drop=True)
# print(df )


############   4      #####
common = df1.merge(df2,on=['product','price'])
print('common:',common)
# 0  monitor    800
# 1     desk    350

# df_all=df1[(~df1.product.isin(common.product))&(~df1.price.isin(common.price))]
df_del=df1[(~df1['product'].isin(common['product']))|(~df1['price'].isin(common['price']))]
print('should be delete:',df_del)
# 0   computer   1200
# 2    printer    200
# 4  just_in_1     10
# these are changed and should be delete

# df_add=df2[(~df2['product'].isin(common['product']))&(~df2['price'].isin(common['price']))]
df_add=df2[(~df2['product'].isin(common['product']))|(~df2['price'].isin(common['price']))]
print('should be insert:',df_add)
# 0   computer    900
# 2    printer    300
# 4  just_in_2    200
# these are new values or records and should be insert

