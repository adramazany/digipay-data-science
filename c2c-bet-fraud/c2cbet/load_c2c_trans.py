""" load_c2c_trans :
    3/5/2022 10:42 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import cx_Oracle
import jdatetime
from pyspark.sql import functions
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType

import dputils
from c2cbet import hlpr, config
from sqlalchemy import create_engine

from c2cbet.helper import spark
from dputils import dateutil, pyspark_funcs


class ETL_C2C_FROM_DB:
    name = "c2c"
    df = None
    startdate=None
    enddate=None

    def __init__(self,yearmonth):
        year=int(yearmonth/100)
        month=int(yearmonth%100)
        self.startdate=jdatetime.date(year,month,1)
        self.enddate=jdatetime.date(year,month,dateutil.month_lastday(self.startdate))

    def etl(self):
        count = self._load()
        print(self.startdate,self.enddate,count)
        self._cleanse()
        self._save()
        return count

    def _load(self):
        sql = "(select aid" \
            ", owner_userid " \
            ", owner_cellnumber " \
            ", amount " \
            ", creationdate " \
            ", expirationdate " \
            ", exercisedate " \
            ", status " \
            ", feecharge " \
            ", type " \
            ", initiator " \
            ", source_postfix " \
            ", source_prefix " \
            ", source_bank_code " \
            ", destination_postfix " \
            ", destination_prefix " \
            ", destination_bank_code " \
            ", destination_cardIndex " \
            ", banktrackingcode rrn " \
            ", gdate " \
            ", ip " \
            ", trackingcode " \
            " from PC_PAYMENTS p " \
            " where STATUS=8" \
            " and type='CARD_TRANSFER'" \
            " and GDATE between to_date('%s','yyyy-mm-dd') and to_date('%s','yyyy-mm-dd')" \
            ") t"%(self.startdate.togregorian().strftime('%Y-%m-%d')
                ,self.startdate.togregorian().strftime('%Y-%m-%d'))

        self.df = spark.read \
            .jdbc(url=config.db_properties['url'], table=sql,properties=config.db_properties)
        self.df.show()
        return self.df.count()

    def _cleanse(self):
        @udf
        def jfrom_unixtime(ts):
            if ts and isinstance(ts,(int)):
                return jdatetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d') , StringType()
            else:
                return None
        self.df = self.df.withColumn("creationdate_d", functions.from_unixtime(self.df.CREATIONDATE)) \
            .withColumn("creationdate_j", jfrom_unixtime(self.df.CREATIONDATE)) \
            .withColumn("expirationdate_d", functions.from_unixtime(self.df.EXPIRATIONDATE)) \
            .withColumn("expirationdate_j", jfrom_unixtime(self.df.EXPIRATIONDATE)) \
            .withColumn("exercisedate_d", functions.from_unixtime(self.df.EXERCISEDATE)) \
            .withColumn("exercisedate_j", jfrom_unixtime(self.df.EXERCISEDATE)) \
            .withColumn("diff_exercise_create", self.df.EXERCISEDATE-self.df.CREATIONDATE )


    def _save(self):
        dfw = self.df.write \
            .mode(config.write_mode) \
            .format(config.write_format)
        # .partitionBy("flight_date_outbound")
        # .bucketBy(42,"visitor_id") \
        # .sortBy("date_time") \
        hlpr.saveOrSaveAsTable(dfw,self.name)


