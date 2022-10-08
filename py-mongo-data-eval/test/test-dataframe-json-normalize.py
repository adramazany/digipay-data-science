import json
import yaml

import pandas as pd


json1 = {'a': ['1', '2'],
        'json_field': ['[{"name":"ali","family":"ram","age":"18","f11":"v11"},{"name":"mohammad","family":"ram","age":"28","f12":"v12"}]',
                      '[{"name":"hassan","family":"ahmadi","age":"118","f21":"v21"},{"name":"reza","family":"ahmadi","age":"128","f22":"v22"}]']
                   }
json2 = "[{'a': '1','json_field':[{'name':'ali','family':'ram','age':'18','f11':'v11'},{'name':'mohammad','family':'ram','age':'28','f12':'v12'}]}," \
        "{'a': '2','json_field':[{'name':'hassan','family':'ahmadi','age':'118','f21':'v21'},{'name':'reza','family':'ahmadi','age':'128','f22':'v22'}]}]"

json3 = [{'a': '1','json_field':{0:{'name':'ali','family':'ram','age':'18','f11':'v11'}
                                ,1:{'name':'mohammad','family':'ram','age':'28','f12':'v12'}}},
        {'a': '2','json_field':{0:{'name':'hassan','family':'ahmadi','age':'118','f21':'v21'}
                                ,1:{'name':'reza','family':'ahmadi','age':'128','f22':'v22'}}}]

# # problem in parsing datetime.datetime(  Loader/FullLoader/UnsafeLoader/BaseLoader/SafeLoader
# str_obj = yaml.load("{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ retried : True,  retryCount : 1,  nextRetryTime :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None}}",yaml.loader.FullLoader)
str_obj = yaml.load("{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ retried : True,  retryCount : 1,  nextRetryTime :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) }}",yaml.loader.FullLoader)
# str_obj = yaml.load("{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ retried : True,  retryCount : 1,  nextRetryTime :  !!timestamp (2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None}}",yaml.loader.FullLoader)
# str_obj = json.loads("{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ retried : True,  retryCount : 1,  nextRetryTime :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None}}")
# str_obj = json.loads("{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ retried : True,  retryCount : 1,  nextRetryTime :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000)}}")

json5 = [{'code':'REGISTER','option':'MANDATORY','processType':'USER_PROCESS','startedDate':'1657002014512','completedDate':'1657002014512'}
    ,{'code':'PROFILE','option':'MANDATORY','processType':'USER_PROCESS','startedDate':'1657029310335','completedDate':'1657029310335'}
    ,{'code':'BANK_SCORE','option':'MANDATORY','processType':'USER_PROCESS','stepResult':'590','startedDate':'1657029318742','completedDate':'1657029318742','additionalInfo':{'creditScoreTrackingCode':'17974723351656942320939'}}
    ,{'code':'UPLOAD','option':'MANDATORY','processType':'USER_PROCESS','documents':[{'title':'تصویر_روی_کارت_ملی','tag':'NATIONAL_CARD_FRONT','option':'MANDATORY_CONDITIONAL','status':'ACCEPTED','uploadDate':'1657029441995','opsActionDate':'1657037632076','docId':'credit-activation/38797dc2-1fff-48b8-be17-16d9e11c5fff.png'},{'title':'تصویر_پشت_کارت_ملی','tag':'NATIONAL_CARD_BACK','option':'MANDATORY_CONDITIONAL','status':'ACCEPTED','uploadDate':'1657029447286','opsActionDate':'1657037637819','docId':'credit-activation/2dd53731-2315-43de-b366-91be669269ff.png'},{'title':'تصویر_روی_کارت_ملیقدیمی','tag':'OLD_NATIONAL_CARD_FRONT','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'تصویر_رسید_کارت_ملی','tag':'NATIONAL_CARD_RECEIPT','option':'MANDATORY_CONDITIONAL','status':'INITIATED'}],'documentType':'NEW_NATIONAL_CARD','stepResult':'NEW','startedDate':'1657029441995','completedDate':'1657037703116'}
    ,{'code':'OPENING_BANK_ACCOUNT','option':'MANDATORY','processType':'SYSTEM_START','startedDate':'1657037703131','completedDate':'1657716916018'}
    ,{'code':'CHEQUE_UPLOAD','option':'MANDATORY','processType':'USER_PROCESS','documents':[{'title':'تصویر_روی_چک','tag':'CHEQUE_FRONT','option':'MANDATORY','status':'INITIATED'},{'title':'تصویر_پشت_چک','tag':'CHEQUE_BACK','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'تصویر_روی_کارت_ملیصاحبچک','tag':'CHEQUE_OWNER_NATIONAL_CARD','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'صفحه_اول_شناسنامه_صاحب_چک','tag':'CHEQUE_OWNER_BIRTH_CERTIFICATE','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'صفحه_اول_یا_دوم_شناسنامه_خودتان','tag':'APPLICANT_BIRTH_CERTIFICATE','option':'MANDATORY_CONDITIONAL','status':'INITIATED'}],'documentType':'OLD_CHEQUE','startedDate':'1657962185760'}
    ,{'code':'OFFLINE_CONTRACT','option':'MANDATORY','processType':'SYSTEM_START','multiDocuments':[{'title':'تصاویر_قرارداد','tag':'OFFLINE_CONTRACT','option':'MANDATORY','status':'INITIATED'}]}
    ,{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS'}
    ,{'code':'REGISTER','option':'MANDATORY','processType':'USER_PROCESS','startedDate':'1657275498295','completedDate':'1657275498295'}
    ,{'code':'PROFILE','option':'MANDATORY','processType':'USER_PROCESS','startedDate':'1657275552803','completedDate':'1657275552803'}
    ,{'code':'BANK_SCORE','option':'MANDATORY','processType':'USER_PROCESS','stepResult':'340','startedDate':'1657275612450','additionalInfo':{'creditScoreTrackingCode':'12872086151657275559195'}}
    ,{'code':'UPLOAD','option':'MANDATORY','processType':'USER_PROCESS','documents':[{'title':'تصویر_روی_کارت_ملی','tag':'NATIONAL_CARD_FRONT','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'تصویر_پشت_کارت_ملی','tag':'NATIONAL_CARD_BACK','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'تصویر_روی_کارت_ملیقدیمی','tag':'OLD_NATIONAL_CARD_FRONT','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'تصویر_رسید_کارت_ملی','tag':'NATIONAL_CARD_RECEIPT','option':'MANDATORY_CONDITIONAL','status':'INITIATED'}]}
    ,{'code':'OPENING_BANK_ACCOUNT','option':'MANDATORY','processType':'SYSTEM_START'}
    ,{'code':'CHEQUE_UPLOAD','option':'MANDATORY','processType':'USER_PROCESS','documents':[{'title':'تصویر_روی_چک','tag':'CHEQUE_FRONT','option':'MANDATORY','status':'INITIATED'},{'title':'تصویر_پشت_چک','tag':'CHEQUE_BACK','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'تصویر_روی_کارت_ملیصاحبچک','tag':'CHEQUE_OWNER_NATIONAL_CARD','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'صفحه_اول_شناسنامه_صاحب_چک','tag':'CHEQUE_OWNER_BIRTH_CERTIFICATE','option':'MANDATORY_CONDITIONAL','status':'INITIATED'},{'title':'صفحه_اول_یا_دوم_شناسنامه_خودتان','tag':'APPLICANT_BIRTH_CERTIFICATE','option':'MANDATORY_CONDITIONAL','status':'INITIATED'}]}
    ,{'code':'OFFLINE_CONTRACT','option':'MANDATORY','processType':'SYSTEM_START','multiDocuments':[{'title':'تصاویر_قرارداد','tag':'OFFLINE_CONTRACT','option':'MANDATORY','status':'INITIATED'}]}
    ,{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':111}
    ,{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{222}}
    ,{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{'f1':'111'}}
    # ,{'code':'WALLET_ACTIVATION','option':'MANDATORY','processType':'USER_PROCESS','additionalInfo':{ 'retried' : True,  'retryCount' : 1,  'nextRetryTime' :  datetime.datetime(2022 , 8: None, 17: None, 7: None, 13: None, 14: None,  614000) : None}}
] +[str_obj]

print('str_obj=',str_obj)
from bson import json_util
print('str_obj.dumps=',json.dumps(str_obj, default=json_util.default))

df1 = pd.DataFrame(json1)
# print('df1=',df1)

json = yaml.load(json2,yaml.loader.UnsafeLoader)
# print(json)
df2 = pd.json_normalize(json)
# print('df2=',df2)

# df3 = pd.json_normalize(json3)
# print('df3=',df3)

# df4 = pd.json_normalize(json4)
# print('df4=',df4)

df5 = pd.json_normalize(json5)
print('df5=',df5.to_string())

print("")

