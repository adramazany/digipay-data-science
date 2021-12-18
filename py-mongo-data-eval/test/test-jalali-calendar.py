
import jdatetime
import datetime
import calendar

import pytz


def add_month_greg(orig_date):
    month_days=[31,28,31,30,31,30,31,31,30,31,30,31]
    if calendar.isleap(orig_date.year) : month_days[1]=29
    # if datetime.isleap(orig_date) : month_days[1]=29
    return orig_date+datetime.timedelta(days=month_days[orig_date.month-1])
def add_month_jalali(orig_date):
    month_days=[31,31,31,31,31,31,30,30,30,30,30,29]
    if orig_date.isleap() : month_days[11]=30
    return orig_date+jdatetime.timedelta(days=month_days[orig_date.month-1])

jdatetime.datetime.now()
jdatetime.date.today()
d=jdatetime.datetime.now()
print(d)
print(d.year)
print(d.month)
print(d.day)
print(d.hour)
print(d.minute)
print(d.second)
print(d.microsecond)


jdatetime.datetime.now().togregorian()

#d1=jdatetime.datetime.fromtimestamp(1602966599580/1000)
ts1=1609314683125
print("timestamp to jdatetime=",jdatetime.datetime.fromtimestamp(ts1/1000))
print("timestamp to datetime=",datetime.datetime.fromtimestamp(ts1/1000))
ts1=1613971971390
print("timestamp to jdatetime=",jdatetime.datetime.fromtimestamp(ts1/1000))
print("timestamp to datetime=",datetime.datetime.fromtimestamp(ts1/1000))

d1=datetime.datetime(2020,2,1)
print("jun start=",d1," timestamp=",d1.timestamp())
d1=add_month_greg(d1)
print("next month=",d1," timestamp=",d1.timestamp())


d1=jdatetime.datetime(1399,12,1)
print("farvardin start=",d1," timestamp=",d1.timestamp())
d1=add_month_jalali(d1)
print("next month=",d1," timestamp=",d1.timestamp())


d1 = datetime.datetime.strptime('2018-12-31',"%Y-%m-%d")
print("load formatted date=",d1)
d1=jdatetime.date.fromtimestamp(d1.timestamp())
print("convert gregorian date=",d1)


d1=datetime.datetime.now()
d2=d1+datetime.timedelta(hours=4,minutes=30)
print("now=",d1.timestamp()*1000, " now+04:30 = ",d2.timestamp()*1000, " +04:30 = ",(d2.timestamp()-d1.timestamp())*1000)


print("start date timestamp to jalali=13981229 jdatetime=%s"%(jdatetime.datetime.fromtimestamp(1584562984576/1000)))
print("start date timestamp to jalali=20200319 datetime=%s"%(datetime.datetime.fromtimestamp(1584562984576/1000).isoformat()))

local_time = datetime.datetime.now().astimezone(pytz.timezone('Asia/Tehran'))
utc_time = datetime.datetime.now().astimezone(pytz.UTC)
print("%s now as tehran local time , %s now as UTC"%(local_time,utc_time))

print("start date timestamp to jalali=13981229 jdatetime(tz_tehran)=%s"%(jdatetime.datetime.fromtimestamp(1584562984576/1000).astimezone(pytz.timezone('Asia/Tehran'))))
print("start date timestamp to jalali=20200319 datetime(tz_tehran)=%s"%(datetime.datetime.fromtimestamp(1584562984576/1000).astimezone(pytz.timezone('Asia/Tehran'))))

print("start date timestamp to jalali=13981229 jdatetime(tz_tehran)=%s"%(jdatetime.datetime.fromtimestamp(1584562984576/1000).astimezone(None)))
print("start date timestamp to jalali=13981229 jdatetime(tz_tehran)=%s"%(jdatetime.datetime.now().astimezone(None)))


print("start date timestamp to jalali=13981229 jdatetime(tz_tehran)=%s"%(jdatetime.datetime.fromtimestamp(1584562984576/1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%Y-%m-%d')))
print("start date timestamp to jalali=13981229 jdatetime(tz_tehran)=%s"%(jdatetime.datetime.fromtimestamp(1584562984576/1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%Y-%m-%d %H:%M:%S')))
print("start date timestamp to [DD-MON-RR HH.MI.SS    AM] jdatetime(tz_tehran)=%s"%(jdatetime.datetime.fromtimestamp(1584562984576/1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y %I:%M:%S    %p')))
print("start date timestamp to [DD-MON-RR HH.MI.SS    AM] datetime(tz_tehran)=%s"%(datetime.datetime.fromtimestamp(1584562984576/1000).astimezone(pytz.timezone('Asia/Tehran')).strftime('%d-%b-%y %I:%M:%S %p')))
#
