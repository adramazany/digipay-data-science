import jdatetime
jdate=jdatetime.datetime(1400,1,1)
cust_reten={'customer':123,'retentionCustomer':456}
sql = "insert into okr_app_cust_retention (gdate,cust_count,cust_retention_count) values (to_date('%s','yyyymmdd','NLS_CALENDAR=PERSIAN'),%s,%s)"%(jdate.strftime('%Y%m%d'), cust_reten["customer"], cust_reten["retentionCustomer"])
print(sql)