""" test_spark_read_from_oracle :
    3/7/2022 6:10 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import cx_Oracle
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

from c2cbet import config

# conf = SparkConf()  # create the configuration
# conf.set("spark.jars", "D:/workspace/digipay-data-science/c2c-bet-fraud/lib/ojdbc6_g.jar")  # set the spark.jars

# conf.setAll([("spark.driver.extraClassPath", "d:/workspace/sepehr/sepehr-web/src/main/webapp/WEB-INF/lib/ojdbc6_g.jar")  # set the spark.jars
#              ,("spark.jars","d:/workspace/sepehr/sepehr-web/src/main/webapp/WEB-INF/lib/ojdbc6_g.jar")])

# conf = pyspark.SparkConf().setAll([('spark.executor.id', 'driver'),
#                                    ('spark.app.id', 'local-1631738601802'),
#                                    ('spark.app.name', 'PySparkShell'),
#                                    ('spark.driver.port', '32877'),
#                                    ('spark.sql.warehouse.dir', 'file:/home/data_analysis_tool/spark-warehouse'),
#                                    ('spark.driver.host', 'localhost'),
#                                    ('spark.sql.catalogImplementation', 'hive'),
#                                    ('spark.rdd.compress', 'True'),
#                                    ('spark.driver.bindAddress', 'localhost'),
#                                    ('spark.serializer.objectStreamReset', '100'),
#                                    ('spark.master', 'local[*]'),
#                                    ('spark.submit.pyFiles', ''),
#                                    ('spark.app.startTime', '1631738600836'),
#                                    ('spark.submit.deployMode', 'client'),
#                                    ('spark.ui.showConsoleProgress', 'true'),
#                                    ('spark.driver.extraClassPath','D:\\workspace\\digipay-data-science\\c2c-bet-fraud\\lib\\ojdbc6_g.jar')])


# spark = SparkSession \
#     .builder.appName(config.appName) \
#     .master(config.master) \
#     .config(conf=conf) \
#     .getOrCreate()
#
# cx_Oracle.init_oracle_client(config.oracle_client_path)
from c2cbet.helper import spark

# df = spark.read.jdbc(url=config.db_properties['url'],table='mongodb.ACTIVITY_TYPE',properties=config.db_properties) \
#     .show()

# sc = pyspark.SparkContext(conf=conf)\
#     .appName(config.appName) \
#     .master(config.master)
# sc.getConf().getAll()
#
# sparkSession = SparkSession (sc)

# sparkDataFrame = spark.read.format("jdbc") \
#     .options(
#     url=config.db_properties['url'],
#     dbtable="mongodb.ACTIVITY_TYPE",
#     user=config.db_properties['username'],
#     password=config.db_properties['password'],
#     driver="oracle.jdbc.OracleDriver"
#     ).load() \
#     .show()

# sparkDataFrame = spark.read.format("jdbc") \
#     .options(**config.db_properties) \
#     .option("dbtable","mongodb.ACTIVITY_TYPE") \
#     .load() \
#     .show()

# sparkDataFrame = spark.read.format("jdbc") \
#     .options(**config.db_properties) \
#     .option("query","select * from mongodb.ACTIVITY_TYPE where id>0") \
#     .load() \
#     .show()

sparkDataFrame = spark.read \
    .jdbc(url=config.db_properties['url'], table="(select * from mongodb.ACTIVITY_TYPE where id>0) t1",properties=config.db_properties) \
    .show()
