import pandas as pd

# d=['_id':{'name':'id','max_length':10,'is_numeric':True}
d={'_id': {'original_name':'id', 'max_length': 24, 'is_numeric': True}
    , 'userId': {'original_name':'userId', 'max_length': 36, 'is_numeric': True}
    , 'state': {'original_name':'state','max_length': 10, 'is_numeric': False}}
print(d)
# print(d['name'])
print("keys=", ",".join(d))

d = {'col1': ['_id',1, 2], 'col2': ['state',3, 4]}
df = pd.DataFrame(data=d)
print('df=',df)
print('df[col1]',df['col1'])
print('df[row _id]', df.loc[df['col1'] == '_id'])


