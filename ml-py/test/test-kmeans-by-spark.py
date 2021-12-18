import pyspark
from pyspark import SparkContext

spark = SparkContext()
print(spark.version)
# java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset.

# df_raw = spark.read().format("com.databricks.spark.csv").load("OnlineRetail.csv")
# df_raw.show(5)