""" test_load_c2c_trans :
    3/11/2022 4:22 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import time
from unittest import TestCase

from c2cbet.load_c2c_trans import ETL_C2C_FROM_DB


class TestETL_C2C_FROM_DB(TestCase):
    def test_etl(self):
        t1 = time.time()
        c2ctrans = ETL_C2C_FROM_DB(140001)
        count = c2ctrans.etl()
        print("count=",count," duration=",(time.time()-t1))
        # self.fail()
