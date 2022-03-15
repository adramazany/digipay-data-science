""" test :
    3/14/2022 5:13 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import json

import requests
import rsa

class Rahkaran:
    username=None
    password=None
    host=None
    rahkaran_session_api = "/sg/Services/Framework/AuthenticationService.svc/session"
    rahkaran_login_api = "/sg3g/x4944f594/Services/Framework/AuthenticationService.svc/login"
    rahkaran_viewvoucher_api = "/sg/Services/Financial/VoucherManagementService.svc/ViewVoucher"
    rahkaran_headers={"Content-Type": "application/json"}
    cookie_filename="rahkaran-http-client.cookies"
    rahkaran_cookies=None

    def __init__(self,username,password,host='http://rahkaran.mydigipay.info'):
        self.username=username
        self.password=password
        self.host=host

        try:
            with open(self.cookie_filename,"r") as fp:
                self.rahkaran_cookies=json.load(fp)
                fp.close()
        except Exception as ex:
            print(ex)
            self.rahkaran_cookies = {"sg-auth-sg": "bf1b36e8-ff25-49bc-a976-570f5cd02bd5", "sg-dummy": "-"}

    def _gen_rahkaran_encrypted_password(self,sessionid, password, pub_key_modulus_hex, pub_key_exponent_hex):
        message = "%s**%s"%(sessionid,password)
        pub_key = rsa.PublicKey(int(pub_key_modulus_hex,16),int(pub_key_exponent_hex,16))

        encMessage = rsa.encrypt(message.encode(),pub_key)
        res = encMessage.hex()
        # print("gen_rahkaran_encrypted_password:",res)
        return res

    def _get_rahkaran_session(self):
        resp = requests.get(self.host+self.rahkaran_session_api)
        if resp.status_code != 200:
            raise Exception('error in get_rahkaran_session {}:{}'.format(resp.status_code,resp.text))
        res = resp.json()
        # print("get_rahkaran_session:",res)
        return res

    def _rahkaran_login(self):
        rahkaran_session = self._get_rahkaran_session()
        rahkaran_encrypted_password = self._gen_rahkaran_encrypted_password(rahkaran_session["id"]
                        , self.password, rahkaran_session["rsa"]["M"], rahkaran_session["rsa"]["E"])

        rahkaran_login_body={"sessionId":rahkaran_session["id"],"username":self.username,"password":rahkaran_encrypted_password}
        resp = requests.post(self.host+self.rahkaran_login_api,headers=self.rahkaran_headers,data=json.dumps(rahkaran_login_body))
        if resp.status_code != 200:
            raise Exception('error in rahkaran_login {}:{}'.format(resp.status_code,resp.text))
        self.rahkaran_cookies = resp.cookies.get_dict()
        with open(self.cookie_filename,"w") as fp:
            json.dump(self.rahkaran_cookies,fp)
        fp.close()
        # print("rahkaran_login:",res)
        return self.rahkaran_cookies

    def get_rahkaran_viewvoucher(self,id):
        rahkaran_viewvoucher_params={"id":id}
        resp = requests.get(self.host+self.rahkaran_viewvoucher_api,params=rahkaran_viewvoucher_params
                             ,headers=self.rahkaran_headers,cookies=self.rahkaran_cookies)
        if resp.status_code != 200:
            print("error in get_rahkaran_viewvoucher without login:{}".format(resp.status_code))
            self._rahkaran_login()
            resp = requests.get(self.host+self.rahkaran_viewvoucher_api,params=rahkaran_viewvoucher_params
                                ,headers=self.rahkaran_headers,cookies=self.rahkaran_cookies)
            if resp.status_code != 200:
                raise Exception('error in get_rahkaran_viewvoucher {}:{}'.format(resp.status_code,resp.text),status_code=resp.status_code)
        res = resp.json()
        # print("get_rahkaran_viewvoucher:",res)
        return res


### test
if __name__ == "__main__":
    rahkaran = Rahkaran(username="DIGIPAY",password="654321")
    voucher = rahkaran.get_rahkaran_viewvoucher(6414)
    print("voucher=",voucher)