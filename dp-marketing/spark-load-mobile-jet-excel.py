
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

excel_path = "csv/mobile-jet.xlsx"
sheet_name = "Sheet1"

df = pd.read_excel(excel_path,sheet_name=sheet_name,index_col=None,header=0)
# df = df.astype({"FirstName": str, "LastName": str})
print(df)

sdf = spark.createDataFrame(df)
sdf.show()

sdf.write.format('jdbc').options(
    url='jdbc:oracle:thin:@10.198.31.51:1521/dgporclw',
    driver='oracle.jdbc.OracleDriver',
    dbtable='tmp_mobile_jet',
    user='mongodb',
    password='OraMon123').mode('overwrite').save()

print("spark write to oracle succeed.")