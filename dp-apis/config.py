""" config :
    4/12/2022 11:48 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"


test_profile = {
    "api_url" : "https://uat.mydigipay.info/digipay/api"
}

live_profile = {
    "api_url" : "https://api.mydigipay.com/digipay/api"

}

active_profile = test_profile
api_url=active_profile["api_url"]

token_url = api_url+"/oauth/token"
api_user = "dp-data"
api_pass = "bXw2Bq+yJ+=6DU#U"
api_auth_basic = "ZHAtZGF0YTpiWHcyQnEreUorPTZEVSNV" # client_id:secret_key
grant_type = "password"
