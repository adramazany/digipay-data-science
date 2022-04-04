""" test_add_col_to_spark_df :
    3/7/2022 5:10 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

import datetime
import jdatetime
from pyspark import Row, SparkConf
from pyspark.sql import SparkSession

from c2cbet import config

from pyspark.sql import functions

spark = SparkSession \
    .builder.appName(config.appName) \
    .master(config.master) \
    .config("spark.sql.warehouse.dir", config.spark_sql_warehouse_dir, SparkConf()) \
    .getOrCreate()

# df = spark.createDataFrame(
#     [(1, "foo"),  # create your data here, be consistent in the types.
#      (2, "bar"),],
#     ["id", "label"]  # add your column names here
# )
data = [('James','Smith','M',3000,1646822155), ('Anna','Rose','F',4100,1646722155),
        ('Robert','Williams','M',6200,1646622155)
        ]
columns = ["firstname","lastname","gender","salary","creationDate"]
df = spark.createDataFrame(data=data, schema = columns)

df.printSchema()
df.show()

#How to add a constant column in a Spark DataFrame?
# In this PySpark article, I will explain different ways of how to add a new column to DataFrame
# using withColumn(), select(), sql(), Few ways include adding a constant column with a default value,
# derive based out of another column, add a column with NULL/None value, add multiple columns

print(">>> Add new constanct column")
from pyspark.sql.functions import lit
df.withColumn("bonus_percent", lit(0.3)) \
    .show()

print(">>> Add New column with NULL")
df.withColumn("DEFAULT_COL", lit(None)) \
    .show()

# def to_jdate()

print(">>> Add column from existing column")
df.withColumn("bonus_amount", df.salary*0.3)\
    .withColumn("creationDate_d", functions.to_date(df.creationDate/1000))\
    .show()
    # .withColumn("creationDate_j", functions.udf() jdatetime.datetime.fromtimestamp(df.creationDate/1000).)\

print(">>> Add column by concatinating existing columns")
from pyspark.sql.functions import concat_ws
df.withColumn("name", concat_ws(",","firstname",'lastname')) \
    .show()


print(">>> Add Column Value Based on Condition")
from pyspark.sql.functions import when
df.withColumn("grade", \
              when((df.salary < 4000), lit("A")) \
              .when((df.salary >= 4000) & (df.salary <= 5000), lit("B")) \
              .otherwise(lit("C")) \
              ).show()

print(">>> Add Column When not Exists on DataFrame")
if 'dummy' not in df.columns:
    df.withColumn("dummy",lit(None)) \
    .show()


print(">>>  Add Multiple Columns using Map")
# Let's assume DF has just 3 columns c1,c2,c3
# apply transformation on these columns and derive multiple columns
# and store these column vlaues into c5,c6,c7,c8,c9,10

# df2 = df.rdd.map(row=>{(c1,c2,c5,c6,c7,c8,c9,c10)})



