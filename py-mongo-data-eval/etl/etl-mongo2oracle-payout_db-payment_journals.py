from bson import ObjectId

from digipay import mongo_to_oracle

MONGO_QUERY_all={}
MONGO_QUERY_1397= {"creationDate":{"$gte":0, "$lt":1553113800000}}               # >=0             and <1398-01-01
MONGO_QUERY_1398= {"creationDate":{"$gte":1553113800000, "$lt":1584649800000}}   # >=1398-01-01    and <1399-01-01
MONGO_QUERY_1399= {"creationDate":{"$gte":1584649800000, "$lt":1616272200000}}   # >=1399-01-01    and <1400-01-01
MONGO_QUERY_1399_1= {"creationDate":{"$gte":1584649800000, "$lt":1587324600000}}
MONGO_QUERY_1399_2= {"creationDate":{"$gte":1587324600000, "$lt":1590003000000}}
MONGO_QUERY_1399_3= {"creationDate":{"$gte":1590003000000, "$lt":1592681400000}}
MONGO_QUERY_1399_4= {"creationDate":{"$gte":1592681400000, "$lt":1595359800000}}
MONGO_QUERY_1399_5= {"creationDate":{"$gte":1595359800000, "$lt":1598038200000}}
MONGO_QUERY_1399_6= {"creationDate":{"$gte":1598038200000, "$lt":1600720200000}}
MONGO_QUERY_1399_7= {"creationDate":{"$gte":1600720200000, "$lt":1603312200000}}
MONGO_QUERY_1399_8= {"creationDate":{"$gte":1603312200000, "$lt":1605904200000}}
MONGO_QUERY_1399_9= {"creationDate":{"$gte":1605904200000, "$lt":1608496200000}}
MONGO_QUERY_1399_10={"creationDate":{"$gte":1608496200000, "$lt":1611088200000}}
MONGO_QUERY_1399_11={"creationDate":{"$gte":1611088200000}}
MONGO_QUERY_1399_9_more= {"creationDate":{"$gte":1605904200000}}
MONGO_QUERY_id= {"_id" : ObjectId('5f5e0246ebeb6105689ccc19')}


MONGO_URI = "mongodb://ops:ops%402020@10.198.110.41:27017/?serverSelectionTimeoutMS=12000000&authSource=admin&authMechanism=SCRAM-SHA-256&connectTimeoutMS=12000000&socketTimeoutMS=12000000&readPreference=secondaryPreferred"
MONGO_DB = "payout_db"
# MONGO_QUERY= MONGO_QUERY_all
MONGO_QUERY= MONGO_QUERY_1397
# MONGO_QUERY= MONGO_QUERY_id
MONGO_COLLECTION="payment_journals"
ORACLE_USER = "stage"
ORACLE_PASS = "stage"
ORACLE_DSN = "172.18.24.84/orcl"
DEST_TABLE_NAME= 'payout_payment_journals'
# COLUMN_TYPE="varchar2(2000)"

def clean_data(data):
    if len(data)>0:
        # if "walletAudits" in data.columns  : data["walletAudits"] = data["walletAudits"].apply(lambda x: x[:2000]  )
        # if "userAudits" in data.columns    : data["userAudits"] = data["userAudits"].apply(lambda x: x[:2000])
        # if "paymentAudits" in data.columns : data["paymentAudits"] = data["paymentAudits"].apply(lambda x: x[:2000]  )
        # if "inquiryAudits" in data.columns : data["inquiryAudits"] = data["inquiryAudits"].apply(lambda x: x[:2000])
        if "walletAudits" in data.columns  : del data['walletAudits']
        if "userAudits" in data.columns    : del data['userAudits']
        if "paymentAudits" in data.columns : del data['paymentAudits']
        if "inquiryAudits" in data.columns : del data['inquiryAudits']

mongo_to_oracle.etl(MONGO_URI,MONGO_DB,MONGO_COLLECTION,MONGO_QUERY,ORACLE_DSN,ORACLE_USER,ORACLE_PASS,DEST_TABLE_NAME
                    ,mode='append',mongo_limit=0,dest_column_type='varchar2(4000)'
                    ,chunk_size=0,fn_clean_data=clean_data)


