import datetime
import re
# json = {"nextRetryTime" :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000)}
import json

obj1 = {"nextRetryTime" :  datetime.datetime(2022 , 8, 17, 7, 13, 14,  614000)}

print( obj1 )
# print( json.dumps(obj1) )
print( json.loads(str(obj1)) )