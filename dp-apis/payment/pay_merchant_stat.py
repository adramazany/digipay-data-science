""" pay-merchant-stat :
    11/2/2022 3:18 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import datetime
import json
import logging
import sys

import flask.json
import numpy as np
import pandas as pd
from flask import Flask
from flask import request,jsonify
from flask_cors import CORS
from flask_restful import Api, Resource
from sqlalchemy import create_engine


app = Flask(__name__)
api = Api(app)
CORS(app)
db_url = 'oracle+cx_oracle://payment_api:payment_api@10.198.31.51:1521/?service_name=dgporclw'

############# logging initiate ##################
_format = '%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s'
_formater = logging.Formatter(_format)
# _level=logging.DEBUG
_level=logging.INFO
logging.basicConfig(level=_level ,format=_format)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
######################

class MerchantStatService:
    # transaction_sql1 = "select status,sum(amt) amt,sum(cnt) cnt from mongodb.FT_PAYMENT_TRANS_DAILY p " \
    #                   " where BUSINESS_ID=%(business_id)s and p.P_DATE between %(date_from)s and %(date_to)s" \
    #                   " and TRANS_TYPE in (10,11) group by status"
    transaction_sql = "select status,sum(amt) amt,sum(cnt) cnt from mongodb.FT_PAYMENT_TRANS_DAILY p " \
                      " where BUSINESS_ID=(select ID from mongodb.BUSINESSES where USER_ID=:business_id) and p.P_DATE between :date_from and :date_to" \
                      " and TRANS_TYPE in (10,11) group by status"
    transaction_json = {"title":"تراکنشها"}

    refund_sql = "select status,sum(amt) amt,sum(cnt) cnt from mongodb.FT_PAYMENT_TRANS_DAILY p " \
                      " where BUSINESS_ID=(select ID from mongodb.BUSINESSES where USER_ID=:business_id) and p.P_DATE between :date_from and :date_to" \
                      " and TRANS_TYPE in (13,33,35) group by status"
    refund_json = {"title":"بازگشت وجه"}

    gateway_day_sql = "select gateway,p_date,status,sum(amt) amt,sum(cnt) cnt from mongodb.FT_PAYMENT_TRANS_DAILY p " \
                      " where BUSINESS_ID=(select ID from mongodb.BUSINESSES where USER_ID=:business_id) and p.P_DATE between :date_from and :date_to" \
                      " and TRANS_TYPE in (10,11) group by gateway,p_date,status order by gateway,p_date"
    gateway_month_sql = "select gateway,substr(p_date,1,7) p_date,status,sum(amt) amt,sum(cnt) cnt from mongodb.FT_PAYMENT_TRANS_DAILY p " \
                      " where BUSINESS_ID=(select ID from mongodb.BUSINESSES where USER_ID=:business_id) and p.P_DATE between :date_from and :date_to" \
                      " and TRANS_TYPE in (10,11) group by gateway,substr(p_date,1,7),status order by gateway,substr(p_date,1,7)"
    gateway_compare_sql = "select gateway,status,sum(amt) amt,sum(cnt) cnt from mongodb.FT_PAYMENT_TRANS_DAILY p " \
                      " where BUSINESS_ID=(select ID from mongodb.BUSINESSES where USER_ID=:business_id) and p.P_DATE between :date_from and :date_to" \
                      " and TRANS_TYPE in (10,11) group by gateway,status order by gateway"
    gateway_json = {"title":"کل تراکنشها به تفکیک درگاه"}

    def __init__(self,db_url):
        self.db_url = db_url
        self.engine = create_engine(url=db_url)

    def _get_params(self,params):
        _params=dict()
        for k in ["business_id","date_from","date_to"]:
            _params[k]=params[k]
        return _params
    def _get_compare_params(self,params):
        if not all(i in params and params[i] for i in ["compare_from","compare_to"]):
            return None
        compare_params=params.to_dict()
        compare_params["date_from"]=params["compare_from"]
        compare_params["date_to"]=params["compare_to"]
        return self._get_params(compare_params)

    def get_transaction_stat(self,params):
        df = pd.read_sql(self.transaction_sql, self.engine, params=self._get_params(params), index_col=['status'])
        logger.debug(df)
        result = self.transaction_json
        if df.empty:
            return result

        result["sum_amount_succeed"] = df['amt']['S']
        result["sum_amount_unsucceed"] = df['amt']['U']
        result["sum_count_succeed"] = df['cnt']['S']
        result["sum_count_unsucceed"] = df['cnt']['U']

        compare_params = self._get_compare_params(params)
        if compare_params:
            df = pd.read_sql(self.transaction_sql, self.engine, params=compare_params, index_col=['status'])
            result["compare_amount"] = df['amt']['S']
            result["compare_rate"] = "{:.0%}".format( result["sum_amount_succeed"]/result["compare_amount"] )

        logger.debug("get_transaction_stat=",result)
        return result

    def get_refund_stat(self,params):
        df = pd.read_sql(self.refund_sql, self.engine, params=self._get_params(params), index_col=['status'])
        logger.debug(df)
        result = self.refund_json
        if df.empty:
            return result

        result["sum_amount_succeed"] = df['amt']['S']
        result["sum_amount_unsucceed"] = df['amt']['U']
        result["sum_count_succeed"] = df['cnt']['S']
        result["sum_count_unsucceed"] = df['cnt']['U']

        compare_params = self._get_compare_params(params)
        if compare_params:
            df = pd.read_sql(self.refund_sql, self.engine, params=compare_params, index_col=['status'])
            result["compare_amount"] = df['amt']['S']
            result["compare_rate"] = "{:.0%}".format( result["sum_amount_succeed"]/result["compare_amount"] )

        logger.debug("get_refund_stat=",result)
        return result

    def _get_gateway_specific(self,gateway_name,df,df_compare):
        df.dropna(inplace=True)
        result={"title":gateway_name}
        df.reset_index("status",inplace=True)
        result["sum_amount_succeed"]    = df.where(df['status']=='S')['amt'].agg("sum")
        result["sum_amount_unsucceed"]  = df.where(df['status']=='U')['amt'].agg("sum")
        result["sum_count_succeed"]     = df.where(df['status']=='S')['cnt'].agg("sum")
        result["sum_count_unsucceed"]   = df.where(df['status']=='U')['cnt'].agg("sum")
        if df_compare is not None and all(df_compare):
            df_compare.dropna(inplace=True)
            result["compare_count"]= df_compare["cnt"]["S"]
            result["compare_rate"] = "{:.0%}".format( result["sum_count_succeed"]/result["compare_count"] )

        result["details"]=[]
        _details_dic = dict()
        for i,row in df.iterrows():
            _gateway_detail = {}
            if row["p_date"] in _details_dic:
                _gateway_detail = _details_dic[row["p_date"]]
            else :
                result["details"].append( _gateway_detail )
                _details_dic[row["p_date"]]= _gateway_detail

            _gateway_detail["title"]=row["p_date"]
            _gateway_detail["date"] =row["p_date"].replace("/","")
            if row['status']=='S':
                _gateway_detail["sum_amount_succeed"]   = row['amt']
                _gateway_detail["sum_count_succeed"]    = row['cnt']
            else:
                _gateway_detail["sum_amount_unsucceed"] = row['amt']
                _gateway_detail["sum_count_unsucceed"]  = row['cnt']

        return result

    def get_gateway_stat(self,params):
        _gateway_sql = self.gateway_month_sql if params["date_range"]=="MONTH" else self.gateway_day_sql
        df = pd.read_sql(_gateway_sql, self.engine, params=self._get_params(params), index_col=['status'])
        logger.debug(df)
        result = self.gateway_json
        if df.empty:
            return result


        df_compare=None
        compare_params = self._get_compare_params(params)
        if compare_params:
            df_compare = pd.read_sql(self.gateway_compare_sql, self.engine, params=compare_params, index_col=['status'])

        result["sum_amount"]= df["amt"].agg("sum")
        result["sum_count"]= df["cnt"].agg("sum")
        if compare_params:
            result["compare_amount"]= df_compare["amt"].agg("sum")
            result["compare_rate"] = "{:.0%}".format( result["sum_amount"]/result["compare_amount"] )

        result["items"]=[]
        for _gateway_name in df['gateway'].unique():
            result["items"].append( self._get_gateway_specific(_gateway_name
                                            ,df.where(df["gateway"]==_gateway_name)
                                            ,df_compare.where(df_compare["gateway"]==_gateway_name) if compare_params else None  ) )

        logger.debug("get_gateway_stat=",result)
        return result

class MerchantStatApi(Resource):
    def np_encoder(self,object):
        if isinstance(object, np.generic):
            return object.item()

    def _get_request_params(self):
        params=request.args
        logger.debug("_get_request_params=",params)
        return params

    def get(self):
        _request_params = self._get_request_params()
        _service = MerchantStatService(db_url)
        result = {}
        result["transaction"] = _service.get_transaction_stat(_request_params)
        result["refund"] = _service.get_refund_stat(_request_params)
        result["gateway"] = _service.get_gateway_stat(_request_params)

        logger.info( result)

        return jsonify(result)

################# PROBLEM SOLVING ##########
# to resolve : {TypeError}Object of type int64 is not JSON serializable
class FlaskJSONEncoder(flask.json.JSONEncoder):
    def default(self,obj):
        if isinstance(obj,(np.integer,np.floating,np.bool_)):
            return obj.item()
        elif isinstance(obj,np.ndarray):
            return obj.tolist()
        elif isinstance(obj,(datetime.datetime,datetime.timedelta)):
            return obj.__str__()
        else:
            return super(FlaskJSONEncoder,self).default(obj)
app.json_encoder = FlaskJSONEncoder # to resolve : {TypeError}Object of type int64 is not JSON serializable
##########

api.add_resource(MerchantStatApi,'/merchant')
@app.route('/')
def homepage():
    return "<h1>Welcome to DP Merchant Stat APIs</h1>" \
           "<form action='merchant'>" \
           "<input name='business_id'   value='eec4d9b0-e5cb-4712-a896-a458f432c8d1'/><br/>" \
           "<input name='date_from'     value='1401/05/01'/><br/>" \
           "<input name='date_to'       value='1401/05/31'/><br/>" \
           "<select name='date_range'>" \
           "<option selected>DAY</option>" \
           "<option >MONTH</option>" \
           "</select><br/>" \
           "<input name='compare_from'  value='1401/04/01'/><br/>" \
           "<input name='compare_to'    value='1401/04/31'/><br/>" \
           "<input type='submit' value='do'/>" \
           "</form>"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8001)


