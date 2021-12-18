from digipay import dateutil
import jdatetime
import datetime


d1=datetime.datetime(2020,2,1)
print("jun start=",d1," timestamp=",d1.timestamp())
d1=dateutil.add_month_greg(d1)
print("next month=",d1," timestamp=",d1.timestamp())


d1=jdatetime.datetime(1399,12,1)
print("farvardin start=",d1," timestamp=",d1.timestamp())
d1=dateutil.add_month_jalali(d1)
print("next month=",d1," timestamp=",d1.timestamp())
