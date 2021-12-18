import jdatetime
import datetime
import requests
from requests.auth import HTTPBasicAuth
from digipay import config


class etl_util:
    ES_URL = "http://localhost:9200"
    ES_USER = 'elastic'
    ES_PASS = 'digipay'
    ES_ETL_CONFIG_INDEX="etl_config"

    query_etl_last_date={
        "query":{}
    }
    update_etl_last_date={
        "index":""
        ,"last_date":""
        ,"last_modified_date":""
    }


    def get_etl_start_date(self,es_index,custom_date=None):
        # get es_index_last_update_date from config index
        return jdatetime.date(1397,7,26)

    def update_etl_last_date(self,es_index,last_date):

        self.update_etl_last_date["index"]=es_index
        self.update_etl_last_date["last_date"]=last_date
        self.update_etl_last_date["last_modified_date"]=datetime.datetime

        resp = requests.get("%s/%s"%(self.ES_URL,self.ES_ETL_CONFIG_INDEX),json=self.update_etl_last_date,auth=config.auth)
        if resp.status_code!=200:
            print(resp.content)
            raise Exception('POST {}'.format(resp.status_code))
        print(resp.json())


