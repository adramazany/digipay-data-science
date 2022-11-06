""" test_pay-merchant-stat :
    11/5/2022 1:23 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from unittest import TestCase

import payment.pay_merchant_stat
from payment.pay_merchant_stat import MerchantStatService


class TestMerchantStatService(TestCase):
    params = {
        "business_id": "1",
        "date_from": "1401/05/01",
        "date_to": "1401/05/31",
        "date_range": "DAY",
        "compare_from": "1401/04/01",
        "compare_to": "1401/04/31"
    }
    def test_get_transaction_stat(self):
        s = MerchantStatService(payment.pay_merchant_stat.db_url)
        r = s.get_transaction_stat(self.params)
        print(r)
        # self.fail()

    def test_get_gateway_stat(self):
        s = MerchantStatService(payment.pay_merchant_stat.db_url)
        r = s.get_gateway_stat(self.params)
        print(r)
