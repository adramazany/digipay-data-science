import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import count,to_utc_timestamp, unix_timestamp, lit, datediff, col, round, \
    mean, min, max, sum, datediff, to_date, udf

spark =SparkContext.getOrCreate()
sqlcontext =SQLContext.getOrCreate(spark)

df = sqlcontext.read.format('com.databricks.spark.csv') \
    .options(header='true',inferschema='true') \
    .load("D:/workspace/digipay-data-science/ml-py/test/OnlineRetail.csv",header=True);
    # .load("../test/OnlineRetail.csv",header=True);

# df = spark.read.load("examples/src/main/resources/people.csv",
#                      format="csv", sep=";", inferSchema="true", header="true")


df.registerTempTable('online_retail_tmp')
df.writeTo('online_retail')
df.write.parquet('online_retail.parquet')
df.write.save('online_retail2.parquet')
df.write.save('online_retail3.parquet', format="parquet")
df.write.saveAsTable('online_retail_tbl')

# df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet'")
df = sqlcontext.sql("SELECT * FROM online_retail.parquet")
df2 = sqlcontext.sql("SELECT * FROM demo.lines")

sqlcontext.sql("create table demo.test(id int,name string)");
sqlcontext.sql("insert into demo.test values (1,'ali'),(2,'hassan'),(3,'mohammad')");
df2 = sqlcontext.sql("SELECT * FROM demo.test")
df2.show()


df.show(5);
# print(df.agg(['min','max']));
print(df.info());
#######################################################
df["TotalPrice"]=df["Quantity"]*df["UnitPrice"]-200 # remove noise data
df["InvoiceDate"]=pd.to_datetime(df["InvoiceDate"],format("%m/%d/%Y %H:%M"))
print(df.info());
#######################################################





