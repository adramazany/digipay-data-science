import datetime
st = "datetime.datetime(2022 , 8, 17, 7, 13, 14,  614000)"
# print(st)
# print(eval(st))

def replace_datetime(text:str,date_format='%Y/%m/%d %H:%M:%S'
                     ,invalid_date_pattern ="[datetime\.]*datetime\(\d{2,4}\s*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,6}\)\s*[: None]*"
                     ,fast_search="datetime("
                     ,replace_with_empty=": None"):
    if fast_search and fast_search not in text:
        return text
    founded_datetimes = re.findall(invalid_date_pattern,st)
    if founded_datetimes:
        converted_datetimes = list(map(lambda x:eval(x.replace(replace_with_empty,'')).strftime(date_format),founded_datetimes))
        splited_others = re.split(invalid_date_pattern,st)
        result=splited_others[0]
        for i in range(1,len(splited_others)):
            result+="'"+converted_datetimes[i-1]+"' "+splited_others[i]
        text=result
    return text

import re
st1 = "kjsdkjhk./,./,.= datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None kjhsdkjhaksdj/,./.,"
st2 = "kjsdkjhk./,./,.= datetime.datetime(2022 , 8, 17, 7, 13, 14,  614000)  kjhsdkjhaksdj/,./.,"
st21 = "kjsdkjhk./,./,.= datetime.datetime(2022,8,17,7,13,14,614000)kjhsdkjhaksdj/,./.,"
st3 = "kjsdkjhk./,./,. datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None kjhsdkjhaksdj/,./.,"
st4 = "kjsdkjhk./,./,.= datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None kjhsdkjhaksdj/,./., datetime.datetime(2020 , 10: None, 1: None, 17: None, 3: None, 4: None,  884000) : None kjhsdkjhaksdj/,./.,"
st41 = "kjsdkjhk./,./,.= datetime.datetime(2020 , 18: None, 7: None, 27: None, 3: None, 4: None,  614000) : None kjhsdkjhaksdj/,./.,"
st42 = "kjsdkjhk./,./,.= datetime.datetime(2020 , 10: None, 1: None, 17: None, 3: None, 4: None,  884000) : None kjhsdkjhaksdj/,./.,"
st5 = "datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None kjhsdkjhaksdj/,./., datetime.datetime(2020 , 10: None, 1: None, 17: None, 3: None, 4: None,  884000) : None"
st6 = "{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ retried : True,  retryCount : 1,  nextRetryTime :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None}}"


# invalid_date_pattern ="datetime\.datetime\(2022 \, 8\: None, 17\: None\, 7\: None\, 13\: None\, 14\: None\,  614000\) \: None"
invalid_date_pattern ="X[datetime\.]*datetime\(\d{2,4}\s*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,2}[: None]*,\s*\d{1,6}\)\s*[: None]*"
# result = re.match(invalid_date_pattern,st1)
st=st6
founded_datetimes = re.findall(invalid_date_pattern,st)
print(founded_datetimes)
converted_datetimes = list(map(lambda x:eval(x.replace(': None','')).strftime('%Y/%m/%d %H:%M:%S'),founded_datetimes))
print(converted_datetimes)
splited_others = re.split(invalid_date_pattern,st)
print(splited_others)
result=splited_others[0]
for i in range(1,len(splited_others)):
    result+="'"+converted_datetimes[i-1]+"' "+splited_others[i]
print(result)
if founded_datetimes:
    print ("HAS")
else:
    print("NOT")

print(replace_datetime(st6))
# print(replace_datetime(st6,date_format="%Y",fast_search=""))
