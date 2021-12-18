from requests.auth import HTTPBasicAuth

ES_URL = "http://localhost:9200"
ES_USER = 'elastic'
ES_PASS = 'digipay'
ES_ETL_CONFIG_INDEX="etl_config"
auth = HTTPBasicAuth(ES_USER, ES_PASS)