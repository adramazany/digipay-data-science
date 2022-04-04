""" pyspark_util :
    3/11/2022 3:27 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import jdatetime
from pyspark.sql import functions
from pyspark.sql.types import StringType

jfrom_unixtime=functions.udf(lambda ts: jdatetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') , StringType() )

