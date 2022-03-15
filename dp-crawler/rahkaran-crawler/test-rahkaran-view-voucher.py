""" test :
    3/14/2022 5:13 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import json

import requests
import rsa

def gen_rahkaran_encrypted_password(sessionid, password, pub_key_modulus_hex, pub_key_exponent_hex):
    message = "%s**%s"%(sessionid,password)
    pub_key = rsa.PublicKey(int(pub_key_modulus_hex,16),int(pub_key_exponent_hex,16))

    encMessage = rsa.encrypt(message.encode(),pub_key)
    res = encMessage.hex()
    print("gen_rahkaran_encrypted_password:",res)
    return res

def get_rahkaran_session():
    rahkaran_session_url = "http://rahkaran.mydigipay.info/sg/Services/Framework/AuthenticationService.svc/session"
    rahkaran_session_headers = {"Host": "rahkaran.mydigipay.info"}
    resp = requests.get(rahkaran_session_url)
    if resp.status_code != 200:
        raise Exception('error in get_rahkaran_session {}:{}'.format(resp.status_code,resp.text))
    res = resp.json()
    print("get_rahkaran_session:",res)
    return res

def rahkaran_login(sessionId,username,encrypted_hex_password):
    rahkaran_login_url = "http://rahkaran.mydigipay.info/sg3g/x4944f594/Services/Framework/AuthenticationService.svc/login"
    rahkaran_login_headers={"Content-Type": "application/json"}
    rahkaran_login_body={"sessionId":sessionId,"username":username,"password":encrypted_hex_password}
    resp = requests.post(rahkaran_login_url,headers=rahkaran_login_headers,data=json.dumps(rahkaran_login_body))
    if resp.status_code != 200:
        raise Exception('error in rahkaran_login {}:{}'.format(resp.status_code,resp.text))
    res = resp.cookies.get_dict()
    with open("rahkaran-http-client.cookies","w") as fp:
        json.dump(rahkaran_cookies,fp)
    fp.close()
    print("rahkaran_login:",res)
    return res

def get_rahkaran_viewvoucher(id,cookies):
    rahkaran_viewvoucher_url = "http://rahkaran.mydigipay.info/sg/Services/Financial/VoucherManagementService.svc/ViewVoucher"
    rahkaran_viewvoucher_params={"id":id}
    rahkaran_viewvoucher_headers={"Content-Type": "application/json"}
    resp = requests.get(rahkaran_viewvoucher_url
                         ,params=rahkaran_viewvoucher_params
                         ,headers=rahkaran_viewvoucher_headers
                         ,cookies=cookies
                         )
    if resp.status_code != 200:
        raise Exception('error in get_rahkaran_viewvoucher {}:{}'.format(resp.status_code,resp.text),status_code=resp.status_code)
    res = resp.json()
    print("get_rahkaran_viewvoucher:",res)
    return res

rahkaran_cookies = {"sg-auth-sg": "bf1b36e8-ff25-49bc-a976-570f5cd02bd5", "sg-dummy": "-"}
try:
    with open("rahkaran-http-client.cookies","r") as fp:
        rahkaran_cookies=json.load(fp)
        fp.close()
except Exception as ex:
    print(ex)


try:
    rahkaran_voucher_id=6414
    rahkaran_voucher = get_rahkaran_viewvoucher(rahkaran_voucher_id,rahkaran_cookies)
except:
    rahkaran_username= "DIGIPAY"
    rahkaran_password= "654321"
    rahkaran_session = get_rahkaran_session()
    rahkaran_encrypted_password = gen_rahkaran_encrypted_password(rahkaran_session["id"], rahkaran_password, rahkaran_session["rsa"]["M"], rahkaran_session["rsa"]["E"])
    rahkaran_cookies = rahkaran_login(rahkaran_session["id"], rahkaran_username, rahkaran_encrypted_password)
    rahkaran_voucher = get_rahkaran_viewvoucher(rahkaran_voucher_id,rahkaran_cookies)


print("SUCCEED.")