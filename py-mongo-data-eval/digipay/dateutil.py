import jdatetime
import datetime
import calendar

def add_month_greg(orig_date):
    month_days=[31,28,31,30,31,30,31,31,30,31,30,31]
    if calendar.isleap(orig_date.year) : month_days[1]=29
    # if datetime.isleap(orig_date) : month_days[1]=29
    return orig_date+datetime.timedelta(days=month_days[orig_date.month-1])
def add_month_jalali(orig_date):
    month_days=[31,31,31,31,31,31,30,30,30,30,30,29]
    if orig_date.isleap() : month_days[11]=30
    return orig_date+jdatetime.timedelta(days=month_days[orig_date.month-1])

def prev_month_jalali(orig_date,count=1):
    month_days=[31,31,31,31,31,31,30,30,30,30,30,29]
    prev_year_int=orig_date.year-(int(count/12))
    if (count%12)>=orig_date.month:prev_year_int-=1
    prev_year=jdatetime.date(prev_year_int,1,1)
    if prev_year.isleap() : month_days[11]=30
    count=count%12
    #prev_month = orig_date.month-1-count if orig_date.month-1-count>=0 else 12+orig_date.month-1-count
    prev_month = orig_date.month-count if orig_date.month-count>0 else 12+orig_date.month-count
    prev_day = orig_date.day if month_days[prev_month-1]>orig_date.day else month_days[prev_month-1]
    return jdatetime.date(prev_year_int,prev_month,prev_day)

def add_month_jalali_multi(orig_date,count):
    for i in range(count):
        orig_date=add_month_jalali(orig_date)
    return orig_date



def fill_dates_from_timestamp(rows, timestamp_field
                              ,timestamp_default_value=None
                              ,data_in_sub_field_source=None
                              ,datetime_name='datetime'
                              ,jalali_date_name='jalali_date'
                              ,jalali_date_format='%Y%m%d'
                              ,add_year_month_day=True
                              ,add_hour_min_sec=True
                              ,add_jalali_year_month_day=True
                              ,year_name='year'
                              ,month_name='month'
                              ,day_name='day'
                              ,hour_name='hour'
                              ,minute_name='minute'
                              ,second_name='second'
                              ,jalali_year_name='jalali_year'
                              ,jalali_month_name='jalali_month'
                              ,jalali_day_name='jalali_day'
                              ):
    for row_main in rows:
        row=row_main
        #todo:must test in mongo2elastic-activities case
        if data_in_sub_field_source : row=row_main[data_in_sub_field_source]

        timestamp=timestamp_default_value
        if row[timestamp_field] and row[timestamp_field]!='' : timestamp==float(row[timestamp_field])

        row[timestamp_field]=timestamp
        date=datetime.datetime.fromtimestamp(timestamp/1000)
        row[datetime_name]= date
        jalali_date=jdatetime.date.fromtimestamp(timestamp/1000)
        row[jalali_date_name]= str(jdatetime.datetime.strftime(jalali_date,jalali_date_format))
        if add_year_month_day :
            row[year_name]= date.year
            row[month_name]= date.month
            row[day_name]= date.day
        if add_hour_min_sec :
            row[hour_name]= date.hour
            row[minute_name]= date.minute
            row[second_name]= date.second
        if add_jalali_year_month_day :
            row[jalali_year_name]= jalali_date.year
            row[jalali_month_name]= jalali_date.month
            row[jalali_day_name]= jalali_date.day
