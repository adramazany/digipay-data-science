
import pandas as pd
# entry point for spark's functionality
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
# configure = SparkConf().setAppName("Test").setMaster("local")
# sc = SparkContext(conf= configure)
# sql = SQLContext(sc)
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Test") \
    .getOrCreate()
    #.config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2") \

print("spark initialized.")

# excel_path = "D:/workspace/digipay-data-science/dp-marketing/csv/2-4_azar_bnpl-lite.xlsx"
# excel_path = "csv/2-4_azar_bnpl-lite.xlsx"
excel_path = "csv/2-4_azar_bnpl.xlsx"
sheet_name = "All"

df = pd.read_excel(excel_path,sheet_name=sheet_name,index_col=None,header=0)
### TypeError: field FirstName: Can not merge type <class 'pyspark.sql.types.StringType'> and <class 'pyspark.sql.types.LongType'>
# df[["a", "b"]] = df[["a", "b"]].apply(pd.to_numeric)
df = df.astype({"FirstName": str, "LastName": str})

print(df)

sdf = spark.createDataFrame(df)

### py4j.protocol.Py4JJavaError: An error occurred while calling o33.load.
# : java.lang.ClassNotFoundException:
# Failed to find data source: com.crealytics.spark.excel. Please find packages at
# sdf = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .option("dataAddress", "'"+sheet_name+"'!A1") \
#     .load(excel_path)

sdf.show()

# url='oracle+cx_oracle://mongodb:OraMon123@10.198.31.51:1521/?service_name=dgporclw',
sdf.write.format('jdbc').options(
    url='jdbc:oracle:thin:@10.198.31.51:1521/dgporclw',
    driver='oracle.jdbc.OracleDriver',
    dbtable='tmp_bnpl',
    user='mongodb',
    password='OraMon123').mode('overwrite').save()

print("spark write to oracle succeed.")