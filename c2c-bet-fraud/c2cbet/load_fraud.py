""" load_fraud :
    3/5/2022 10:42 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from pyspark.sql.functions import lit

from c2cbet import hlpr, config

class FraudRealData:
    name = "c2c_bet"
    df = None
    filePath = None

    def etl(self,provider,yearmonth, filePath, dataAddress="Sheet1"):
        self.filePath = filePath
        count = self._load(filePath, dataAddress)
        self.df.withColumn("provider", lit(provider))
        self.df.withColumn("yearmonth", lit(yearmonth))
        print(filePath,count)
        self._cleanse()
        self._save()
        return count

    def _load(self,filePath,dataAddress):
        self.df = hlpr.readExcel(filePath, dataAddress)
        return self.df.count()

    def _cleanse(self):
        for x in config.read_excel_required_columns:
            if not x in self.df.schema.names :
                raise Exception("the imported excel file %s does not have column %s"%(self.filePath,x))

    def _save(self):
        dfw = self.df.write \
            .mode(config.write_mode) \
            .format(config.write_format)
        # .partitionBy("flight_date_outbound")
        # .bucketBy(42,"visitor_id") \
        # .sortBy("date_time") \
        hlpr.saveOrSaveAsTable(dfw,self.name)


