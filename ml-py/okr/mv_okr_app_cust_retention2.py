import logging
import numpy as np
from sqlalchemy import create_engine
import pandas as pd
import jdatetime
import cx_Oracle
import sys

_format = '%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s'
_formater = logging.Formatter(_format)
_level=logging.DEBUG
# _level=logging.INFO
logging.basicConfig(level=_level ,format=_format)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
fileHandler = logging.FileHandler(__file__+".log")
fileHandler.setFormatter(_formater)
fileHandler.setLevel(_level)
logger.addHandler(fileHandler)

cx_Oracle.init_oracle_client('d:/app/oracle_instantclient_19_10')
# cx_Oracle.init_oracle_client('/home/oracle/oracle_instantclient_21_1')

class MyNumpy :
      startdate=None
      enddate=None
      jdate_columns = None
      columns = dict()
      cellnumbers = dict()
      ar = np.NaN

      df = None
      def __init__(self,startdate,enddate,ar_init_size=0):
            self.startdate=startdate
            self.enddate=enddate
            delta = enddate-startdate
            self.jdate_columns = [ (startdate+jdatetime.timedelta(days=i)).strftime('%Y%m%d') for i in range(delta.days+1)]
            self.columns = dict(zip(self.jdate_columns,range(0,len(self.jdate_columns))))
            self.ar = np.zeros((ar_init_size,len(self.jdate_columns)),dtype='int8')

      def resize(self,ar_add_len):
            # self.ar.shape = (self.ar.shape[0]+ar_add_len,self.ar.shape[1])
            self.ar.resize((self.ar.shape[0]+ar_add_len,self.ar.shape[1]))
      def jnextmonth(self,jdate):
            month_days=[31,31,31,31,31,31,30,30,30,30,30,29]
            if jdate.isleap() : month_days[11]=30
            return jdate+jdatetime.timedelta(days=month_days[jdate.month-1])
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
            return jdatetime.datetime(prev_year_int,prev_month,prev_day)

      def setValue(self,cellnumber,jdate):
            nextmonth = self.jnextmonth(jdate)
            if nextmonth>self.enddate:
                  nextmonth=self.enddate
            delta = nextmonth-jdate

            if  cellnumber not in self.cellnumbers:
                  self.cellnumbers[cellnumber]=len(self.cellnumbers)
            i_cellnumber=self.cellnumbers.get(cellnumber)

            jdate_str = jdate.strftime('%Y%m%d')
            i_jdate = self.columns[jdate_str]
            # self.ar.resize(len(self.ar)+1)
            for i in range(delta.days+1):
                  if self.ar[i_cellnumber,i_jdate+i]!=1:
                        self.ar[i_cellnumber,i_jdate+i]=1

      def make_df(self):
            self.df=pd.DataFrame(self.ar,columns=self.jdate_columns)
      def countCustomer(self,jdate):
            jdate_str = jdate.strftime('%Y%m%d')
            return self.df.agg({jdate_str:['sum']})[jdate_str]['sum']
      def countRetentionCustomer(self,jdate):
            jdate_str = jdate.strftime('%Y%m%d')
            jdate_prevmonth_str = self.jprevmonth(jdate).strftime('%Y%m%d')
            # return self.df.agg('count',axis=[jdate_str,jdate_prevmonth_str] )
            if jdate_str in self.df and jdate_prevmonth_str in self.df :
                  # return self.df.groupby([jdate_str,jdate_prevmonth_str]).size()[True][True]
                  _groupby = self.df.groupby([jdate_str,jdate_prevmonth_str]).size()
                  if (1,1) in _groupby:
                        return _groupby[1][1]
                  else:
                        return 0
            else:
                  return 0


logger.info('creating engine...')
engine = create_engine("oracle+cx_oracle://mongodb:Mongo123@172.18.24.84:1521/?service_name=ORCL")

logger.info('initialize data...')
# startdate=jdatetime.datetime(1400,1,1)
chunk_size=1000

if len(sys.argv)<2 :
      print("syntx : python okr_app_cust_retention.py 14000101 [14001030]")
      raise()
elif len(sys.argv)==2:
      # startdate=jdatetime.datetime(1399,10,1)
      startdate=jdatetime.datetime.strptime(sys.argv[1],'%Y%m%d')
      enddate  =startdate
else:
      startdate=jdatetime.datetime.strptime(sys.argv[1],'%Y%m%d')
      enddate  =jdatetime.datetime.strptime(sys.argv[2],'%Y%m%d')

one_month_ago_startdate = MyNumpy.jprevmonth(MyNumpy,startdate)
two_months_ago_startdate = MyNumpy.jprevmonth(MyNumpy,MyNumpy.jprevmonth(MyNumpy,startdate))
logger.info('one_month_ago_startdate=%s,two_months_ago_startdate=%s'%(one_month_ago_startdate,two_months_ago_startdate))

data = MyNumpy(two_months_ago_startdate,enddate)

sql = " select distinct to_char(GDATE,'yyyymmdd','NLS_CALENDAR=PERSIAN') pdate,SOURCE_CELLNUMBER" \
      " from ft_app_trans" \
      " where GDATE between to_date('%s','yyyymmdd','NLS_CALENDAR=PERSIAN')" \
      " and to_date('%s','yyyymmdd','NLS_CALENDAR=PERSIAN')"%(two_months_ago_startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d'))

logger.info('get the connection...')
counter=1
total=0
with engine.connect() as conn:
      logger.info('executing query=%s'%(sql))
      rs = conn.execute(sql)
      while True:
            logger.debug('fetchmany %s-th chunk of records'%(counter))
            chunk = rs.fetchmany(chunk_size)

            if not chunk:
                  break

            logger.debug('adding %s records to numpy'%(len(chunk)))
            data.resize(len(chunk))

            for row in chunk:
                  tnx_pdate = jdatetime.datetime.strptime(row[0],'%Y%m%d')
                  cellnumber = row[1]
                  data.setValue(cellnumber,tnx_pdate)

            logger.debug(data.ar.dtype)

            total+=len(chunk)
            if counter%10==0:
                  logger.info("%s records fetched and placed in next month ranges."%(total))

            counter+=1

print(data.ar)
logger.info('make_df....')
data.make_df()
print(data.df.info())

logger.info('start calculation ...')
delta = enddate - startdate
list_cust_reten=dict()
for i in range(delta.days+1):
      jdate = startdate+jdatetime.timedelta(days=i)
      customer = data.countCustomer(jdate)
      retentionCustomer = data.countRetentionCustomer(jdate)
      jdate_str = jdate.strftime('%Y%m%d')
      list_cust_reten[jdate_str] = {'customer':customer,'retentionCustomer':retentionCustomer}
      logger.info("jdate=%s, countCustomer=%s, countRetentionCustomer=%s"%(jdate_str,customer,retentionCustomer))

logger.info('update database ...')
with engine.connect() as conn:
      try:
            # conn.autocommit(False)
            sql=" delete from okr_app_cust_retention where gdate between " \
                " to_date('%s','yyyymmdd','NLS_CALENDAR=PERSIAN') and to_date('%s','yyyymmdd','NLS_CALENDAR=PERSIAN') "%(startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d'))
            logger.info(sql)
            conn.execute(sql)

            for i in range(delta.days+1):
                  jdate = startdate+jdatetime.timedelta(days=i)
                  jdate_str = jdate.strftime('%Y%m%d')
                  cust_reten = list_cust_reten[jdate_str]
                  sql = "insert into okr_app_cust_retention (gdate,cust_count,cust_retention_count) values (to_date('%s','yyyymmdd','NLS_CALENDAR=PERSIAN'),%s,%s)"%(jdate_str, cust_reten["customer"], cust_reten["retentionCustomer"])
                  logger.debug(sql)
                  conn.execute(sql)

# 2022-01-02 17:38:15,942 - 20920 - __main__ - INFO - jdate=14000401, countCustomer=3508628, countRetentionCustomer=277914
# 2022-01-02 17:38:15,942 - 20920 - __main__ - INFO - update database ...
# 2022-01-02 17:38:15,942 - 20920 - __main__ - ERROR - roolback on exception in update okr_app_cust_retention table! ex=not all arguments converted during string formatting

            logger.info('succeed.')
            # conn.commit()
      except Exception as ex:
            # conn.rollback()
            logger.error("exception in update okr_app_cust_retention table! ex=%s"%(ex))

# (duration=5m)  jdate=14000101, countCustomer=1,834,519, countRetentionCustomer=280,551
# 14000101  425971	280551

# 14000101	425971	280551
# 14001001	366920	201578
# 14001002	366135	201380
# 14001003	366096	202677
# 14001004	365010	202603
# 14001005	364180	203917
# 14001006	364542	204107
# 14001007	372827	204966
# 14001008	375090	205268
# 14001009	374743	204565
# 14001010	374448	204529
# 14001011	377349	204655
# 14001012	379725	204193
# 14001013	382117	205012

