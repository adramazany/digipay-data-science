""" test_load_fraud :
    3/5/2022 1:52 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from unittest import TestCase

from c2cbet import config
from c2cbet.load_fraud import FraudRealData


class TestFraudRealData(TestCase):

    def setUp(self) :
        config.c2cbet_src_path="../fraud-data/"

    def test_etl_sadad140008(self):
        FraudRealData().etl('sadad',140008,config.c2cbet_src_path+"140008-sadad.xlsx")
        # 372

    def test_etl_sadad140009(self):
        FraudRealData().etl('sadad',140009,config.c2cbet_src_path+"140009-sadad.xlsx")
        # 355

    def test_etl_eghtedadnovin140009(self):
        FraudRealData().etl('eghtedadnovin',140009,config.c2cbet_src_path+"140009-eghtedadnovin.xlsx")
        # 146
