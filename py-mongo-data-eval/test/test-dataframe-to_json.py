""" test-dataframe-to_json :
    11/2/2022 5:27 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import pandas as pd

df = pd.DataFrame({'STATUS': ['U', 'S'],
                   'AMT': [3074388479006, 19106424430077],
                   'CNT': [154021, 1688508]})
df.set_index("STATUS", inplace = True)
print(df)
print("df['AMT']['U']=",df['AMT']['U'])
print("df['CNT']['S']=",df['CNT']['S'])

json = {
    "transaction" : {
        "title":"تراکنشها"
        ,"sum_amount_succeed":df['AMT']['S']
        ,"sum_amount_unsucceed":df['AMT']['U']
        ,"sum_count_succeed":df['CNT']['S']
        ,"sum_count_unsucceed":df['CNT']['U']
        ,"compare_rate":+2.5
        ,"compare_amount":157000000
    }}
print(json)