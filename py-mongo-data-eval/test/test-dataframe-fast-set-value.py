import json
import time

import yaml

import pandas as pd
import numpy as np

columns=["c%s"%(j) for j in range(1000) ]

chunk=10000
######################################################
########### unbeleiveble faster then dataframe
ar = np.zeros((chunk,len(columns)))
t1=time.time()
for i in range(chunk):
    ar[i]=[i*j%10000 for j in range(len(columns)) ]
print("np[i](%s) :"%(chunk),(time.time()-t1),' s')
print(ar)

######################################################
########### unbeleiveble faster then dataframe, 2 times slower than adding full row
ar = np.zeros((chunk,len(columns)))
t1=time.time()
for i in range(chunk):
    for j in range(len(columns)):
        ar[i,j]= i*j%10000
print("np[i](%s) :"%(chunk),(time.time()-t1),' s')
print(ar)

######################################################
# df = pd.DataFrame(index=['cellnumber'],columns=columns)
# t1=time.time()
# for i in range(chunk):
#     for j in range(len(columns)):
#         df._set_value(i,"c%s"%(j),i*j%10000)
# print("_set_value(%s) by row and column name :"%(chunk),(time.time()-t1),' s')
# print(df.info)

######################################################
df = pd.DataFrame(columns=columns)
t1=time.time()
for i in range(chunk):
    for j in range(len(columns)):
        df._set_value(i,j,i*j%10000)
print("_set_value(%s) without column and row name :"%(chunk),(time.time()-t1),' s')
print(df.info)

######################################################
# df = pd.DataFrame(index=['cellnumber'],columns=columns)
# t1=time.time()
# for i in range(chunk):
#     for j in range(len(columns)):
#         df._set_value(i,"c%s"%(j),i*j%10000)
# print("_set_value(%s) :"%(chunk),(time.time()-t1),' s')
# print(df.info)

######################################################
########### more than 10 times slower ########################
# df = pd.DataFrame(index=['cellnumber'],columns=columns)
# t1=time.time()
# for i in range(chunk):
#     if not i in df.index:
#         df.loc[i]={}
#     for j in range(len(columns)):
#         df.loc[i]["c%s"%(j)]=i*j%10000
# print("df.loc[i][c](%s) :"%(chunk),(time.time()-t1),' s')
# print(df.info)

######################################################
################ more then 2 times slower
# df = pd.DataFrame(index=['cellnumber'],columns=columns)
# t1=time.time()
# for i in range(chunk):
#     for j in range(len(columns)):
#         df.loc[i,"c%s"%(j)]=i*j%10000
# print("df.loc[c,i](%s) :"%(chunk),(time.time()-t1),' s')
# print(df.info)

######################################################
################ more then 3 times slower
# df = pd.DataFrame(index=['cellnumber'],columns=columns)
# t1=time.time()
# for i in range(chunk):
#     obj={}
#     for j in range(len(columns)):
#         obj["c%s"%(j)]=i*j%10000
#     df.loc[i]=obj
# print("df.loc[i]=obj(%s) :"%(chunk),(time.time()-t1),' s')
# print(df.info)

######################################################
################ more then 2 times slower
# df = pd.DataFrame(index=['cellnumber'],columns=columns)
# t1=time.time()
# for i in range(chunk):
#     obj={}
#     for j in range(len(columns)):
#         obj["c%s"%(j)]=i*j%10000
#     newrow = pd.Series(obj, index=['cellnumber'],name=i )
#     df.append(newrow)
# print("df.loc[i]=obj(%s) :"%(chunk),(time.time()-t1),' s')
# print(df.info)
#
