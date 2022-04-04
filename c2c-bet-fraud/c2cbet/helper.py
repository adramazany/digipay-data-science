"""helper.py:
    Provide common methods to help simplicity and integrity
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import json
import os

import pandas as pd
from pyspark import SparkConf, Row
from pyspark.sql import SparkSession

from c2cbet import config

spark = SparkSession \
    .builder.appName(config.appName) \
    .master(config.master) \
    .config("spark.sql.warehouse.dir", config.spark_sql_warehouse_dir, SparkConf()) \
    .getOrCreate()



class Helper:

    def __init__(self):
        # self._load_config()
        pass

    def recreate_spark(self):
        SparkSession.getActiveSession().stop()
        builder = SparkSession \
            .builder.appName(config.appName) \
            .master(config.master)
        if config.hive_metastore_uris:
            builder.config("hive.metastore.uris", config.hive_metastore_uris, SparkConf())
        if config.spark_sql_warehouse_dir:
            builder.config("spark.sql.warehouse.dir", config.spark_sql_warehouse_dir, SparkConf())

        spark = builder.getOrCreate()
        return spark

    def saveOrSaveAsTable(self, dfw, tablename):
        if config.spark_sql_table_prefix != "":
            dfw.save(tablename)
        else:
            dfw.saveAsTable(tablename)

    def _readExcel1(self, filePath, dataAddress="'Sheet1'!A1"):
        dfr = spark.read.format(config.read_excel_format) \
            .option("dataAddress", dataAddress) \
            .option("header", "true") \
            .option("inferSchema", "true")
        # options = config.read_excel_options
        # for k in options:
        #     dfr.option(k,options[k])
        return dfr.load(filePath)

    def readExcel(self, filePath, sheet_name="Sheet1"):
        df2 = pd.read_excel(filePath,sheet_name=sheet_name)
        return spark.createDataFrame(df2)


