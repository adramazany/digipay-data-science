import jdatetime
import datetime
import calendar

month_days=[31,28,31,30,31,30,31,31,30,31,30,31]
jmonth_days=[31,31,31,31,31,31,30,30,30,30,30,29]

def month_lastday(date):
    if date.month==2 and calendar.isleap(date.year):
        return 29
    return month_days[date.month - 1]

def jmonth_lastday(jdate):
    if jdate.month==12 and jdate.isleap():
        return 30
    return jmonth_days[jdate.month-1]

def month_add(date):
    return date + datetime.timedelta(days=month_lastday(date))

def jmonth_add(jdate):
    return jdate + jdatetime.timedelta(days=jmonth_days(jdate))

def jmonth_prev(jdate, count=1):
    month_days=[31,31,31,31,31,31,30,30,30,30,30,29]
    prev_year_int= jdate.year - (int(count / 12))
    if (count%12)>=jdate.month:prev_year_int-=1
    prev_year=jdatetime.date(prev_year_int,1,1)
    if prev_year.isleap() : month_days[11]=30
    count=count%12
    #prev_month = orig_date.month-1-count if orig_date.month-1-count>=0 else 12+orig_date.month-1-count
    prev_month = jdate.month - count if jdate.month - count > 0 else 12 + jdate.month - count
    prev_day = jdate.day if month_days[prev_month - 1] > jdate.day else month_days[prev_month - 1]
    return jdatetime.date(prev_year_int,prev_month,prev_day)

def jmonth_add_many(jdate, count):
    for i in range(count):
        jdate=jmonth_add(jdate)
    return jdate


