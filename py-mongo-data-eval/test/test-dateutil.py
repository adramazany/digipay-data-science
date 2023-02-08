from datetime import datetime, date

import jdatetime
from digipay import dateutil
print("############ from string ################")
print("from string=",datetime.strptime("12/1/2010 8:26","%d/%m/%Y %H:%M"))
print("############ now ################")
date_jalali=jdatetime.datetime.now()
print("date_jalali=",date_jalali)
print("prev_month_jalali=",dateutil.prev_month_jalali(date_jalali))
print("prev_month_jalali_1m=",dateutil.prev_month_jalali(date_jalali,1))
print("prev_month_jalali_2m=",dateutil.prev_month_jalali(date_jalali,2))
print("prev_month_jalali_3m=",dateutil.prev_month_jalali(date_jalali,3))
print("prev_month_jalali_6m=",dateutil.prev_month_jalali(date_jalali,6))
print("prev_month_jalali_12m=",dateutil.prev_month_jalali(date_jalali,12))
print("prev_month_jalali_12m=",dateutil.prev_month_jalali(date_jalali,24))

print("############ month-start ################")
date_jalali=jdatetime.datetime(1399,1,1)
print("date_jalali=",date_jalali)
print("prev_month_jalali=",dateutil.prev_month_jalali(date_jalali))
print("prev_month_jalali_1m=",dateutil.prev_month_jalali(date_jalali,1))
print("prev_month_jalali_2m=",dateutil.prev_month_jalali(date_jalali,2))
print("prev_month_jalali_3m=",dateutil.prev_month_jalali(date_jalali,3))
print("prev_month_jalali_6m=",dateutil.prev_month_jalali(date_jalali,6))
print("prev_month_jalali_12m=",dateutil.prev_month_jalali(date_jalali,12))
print("prev_month_jalali_12m=",dateutil.prev_month_jalali(date_jalali,24))

pytz.tzinfo()
print("############ month-end ################")
date_jalali=jdatetime.datetime(1399,1,31)
print("date_jalali=",date_jalali)
print("prev_month_jalali=",dateutil.prev_month_jalali(date_jalali))
print("prev_month_jalali_1m=",dateutil.prev_month_jalali(date_jalali,1))
print("prev_month_jalali_2m=",dateutil.prev_month_jalali(date_jalali,2))
print("prev_month_jalali_3m=",dateutil.prev_month_jalali(date_jalali,3))
print("prev_month_jalali_6m=",dateutil.prev_month_jalali(date_jalali,6))
print("prev_month_jalali_12m=",dateutil.prev_month_jalali(date_jalali,12))
print("prev_month_jalali_12m=",dateutil.prev_month_jalali(date_jalali,24))


print("############ month-end ################")
date_jalali=jdatetime.datetime(1400,7,30)
print("date_jalali=",dateutil.prev_month_jalali(date_jalali))
