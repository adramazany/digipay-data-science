""" test_util :
    3/9/2022 10:59 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import jdatetime

jdate = jdatetime.date(1400,1,1)
print(jdate)
print(jdate.strftime('%Y%m%d'))
print(jdate.togregorian().strftime('%Y%m%d'))

yearmonth=140001
print(int(yearmonth/100))
print(str(yearmonth)[0:4])

print(int(yearmonth%100))
print(str(yearmonth)[4:6])
