import logging

from sqlalchemy import create_engine
import pandas as pd
import jdatetime
import cx_Oracle

class MyDataFrame :
      startdate=None
      enddate=None
      columns = None
      df = None
      def __init__(self,startdate,enddate):
            self.startdate=startdate
            self.enddate=enddate
            delta = enddate-startdate
            self.columns = [ (startdate+jdatetime.timedelta(days=i)).strftime('%Y%m%d') for i in range(delta.days+1)]
            self.df = pd.DataFrame(index=['cellnumber'],columns=self.columns)

      def jnextmonth(self,jdate):
            month_days=[31,31,31,31,31,31,30,30,30,30,30,29]
            if jdate.isleap() : month_days[11]=30
            return jdate+jdatetime.timedelta(days=month_days[jdate.month])
      def jprevmonth(self,orig_date,count=1):
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

      def setValue(self,cellnumber,jdate):
            # if not cellnumber in self.df.index:
                  # newrow = pd.Series({},name=cellnumber)
                  # self.df.append(newrow)
                  # newrow = pd.Series([cellnumber], index=['cellnumber'] )
                  # self.df.append( newrow,ignore_index=True);
                  # self.df.loc[cellnumber]={}
            # row = self.df.loc[cellnumber]
            # print('row=',row)
            nextmonth = self.jnextmonth(self.jnextmonth(jdate))
            if nextmonth>self.enddate:
                  nextmonth=self.enddate
            delta = nextmonth-jdate
            for i in range(delta.days+1):
                  # row[(jdate+jdatetime.timedelta(days=i)).strftime('%Y%m%d')]=1
                  self.df._set_value(cellnumber, (jdate+jdatetime.timedelta(days=i)).strftime('%Y%m%d') , True)
                  # 10,000 => 2m 11s
                  # self.df[cellnumber, (jdate+jdatetime.timedelta(days=i)).strftime('%Y%m%d') , True)
      def countCustomer(self,jdate):
            jdate_str = jdate.strftime('%Y%m%d')
            return self.df.agg({jdate_str:['count']})[jdate_str]['count']
      def countRetentionCustomer(self,jdate):
            jdate_str = jdate.strftime('%Y%m%d')
            jdate_prevmonth_str = self.jprevmonth(jdate).strftime('%Y%m%d')
            # return self.df.agg('count',axis=[jdate_str,jdate_prevmonth_str] )
            if jdate_str in self.df and jdate_prevmonth_str in self.df :
                  return self.df.groupby([jdate_str,jdate_prevmonth_str]).size()[True][True]
            else:
                  return 0

logging.basicConfig(level=logging.INFO ,format='%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info('creating engine...')
cx_Oracle.init_oracle_client('d:/app/oracle_instantclient_19_10')
engine = create_engine("oracle+cx_oracle://mongodb:Mongo123@172.18.24.84:1521/?service_name=ORCL")

logger.info('initialize data...')
startdate=jdatetime.datetime(1400,1,1)
enddate=jdatetime.datetime(1400,2,31)
data = MyDataFrame(startdate,enddate)
sql = " select distinct to_char(F_GDATE,'yyyymmdd','NLS_CALENDAR=PERSIAN') pdate,SOURCE_CELLNUMBER from mv_app_trans2" \
      " where F_GDATE=to_date('13991001','yyyymmdd','NLS_CALENDAR=PERSIAN')"

logger.info('get the connection...')
counter=1
chunk_size=10000
with engine.connect() as conn:
      logger.info('executing query=%s'%(sql))
      rs = conn.execute(sql)
      while True:
            logger.info('fetchmany %s chunk of:%s records'%(counter,chunk_size))
            chunk = rs.fetchmany(chunk_size)

            logger.info('adding %s records to dataframe'%(len(chunk)))
            if not chunk:
                  break
            for row in chunk:
                  tnx_pdate = jdatetime.datetime.strptime(row[0],'%Y%m%d')
                  cellnumber = row[1]
                  data.setValue(cellnumber,tnx_pdate)
            counter+=1

print(data.df.info)
logger.info('start calculation....')

delta = enddate-startdate
for i in range(delta.days+1):
      jdate = startdate+jdatetime.timedelta(days=i)
      customer = data.countCustomer(jdate)
      retentionCustomer = data.countRetentionCustomer(jdate)
      logger.info("jdate=%s, countCustomer=%s, countRetentionCustomer="%(jdate.strftime('%Y%m%d'),customer,retentionCustomer))







# data.setValue('090001',(jdatetime.date.today().__add__(jdatetime.timedelta(days=-60))))
# data.setValue('090001',startdate)
# data.setValue('090002',startdate+jdatetime.timedelta(days=1))
# data.setValue('090003',startdate+jdatetime.timedelta(days=2))
# print(data.df.info)
# v1 = data.countCustomer(startdate)
# print('v1=',v1)
# v2 = data.countCustomer(enddate)
# print('v2=',v2)
# v3 = data.countRetentionCustomer(jdatetime.date(1400,2,1))
# print('v3=',v3)
# v4 = data.countRetentionCustomer(jdatetime.date(1400,2,2))
# print('v4=',v4)
# v5 = data.countRetentionCustomer(jdatetime.date(1400,2,3))
# print('v5=',v5);



# 2021-12-26 19:25:14,077 - 19896 - __main__ - INFO - fetchmany 1 chunk of:1000 records
# 2021-12-26 19:25:23,748 - 19896 - __main__ - INFO - fetchmany 2 chunk of:1000 records
# 2021-12-26 19:25:35,100 - 19896 - __main__ - INFO - fetchmany 3 chunk of:1000 records
# 2021-12-26 19:25:48,117 - 19896 - __main__ - INFO - fetchmany 4 chunk of:1000 records
# 2021-12-26 19:26:03,059 - 19896 - __main__ - INFO - fetchmany 5 chunk of:1000 records
# 2021-12-26 19:26:19,029 - 19896 - __main__ - INFO - fetchmany 6 chunk of:1000 records
# 2021-12-26 19:26:36,454 - 19896 - __main__ - INFO - fetchmany 7 chunk of:1000 records
# 2021-12-26 19:26:55,408 - 19896 - __main__ - INFO - fetchmany 8 chunk of:1000 records
# 2021-12-26 19:27:15,956 - 19896 - __main__ - INFO - fetchmany 9 chunk of:1000 records
# 2021-12-26 19:27:37,860 - 19896 - __main__ - INFO - fetchmany 10 chunk of:1000 records
# 2021-12-26 19:28:01,148 - 19896 - __main__ - INFO - fetchmany 11 chunk of:1000 records
# 2021-12-26 19:28:26,832 - 19896 - __main__ - INFO - fetchmany 12 chunk of:1000 records
# 2021-12-26 19:28:53,593 - 19896 - __main__ - INFO - fetchmany 13 chunk of:1000 records
# 2021-12-26 19:29:21,957 - 19896 - __main__ - INFO - fetchmany 14 chunk of:1000 records