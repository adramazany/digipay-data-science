""" test_load_fraud_to_db :
    3/16/2022 9:11 AM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from pyspark.sql.functions import lit

from c2cbet import hlpr, config


class FraudRealData2DB:
    name = "c2c_bet"
    df = None
    filePath = None
    db_props=None

    def __init__(self,db_props):
        self.db_props=db_props

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
        self.df = self.df.replace(float('nan'),None)


    def _save(self):
        dfw = self.df.write \
            .mode(config.write_mode) \
            .format("jdbc") \
            .options(**self.db_props) \
            .option("dbtable",self.name) \
            .save(self.name)


if __name__=="__main__" :
    config.c2cbet_src_path="../fraud-data/"
    inputs = [{"provider":'sadad'        ,"yearmonth":140008,"filePath":config.c2cbet_src_path+"140008-sadad.xlsx"        }
            ,{"provider":'sadad'        ,"yearmonth":140009,"filePath":config.c2cbet_src_path+"140009-sadad.xlsx"        }
            ,{"provider":'eghtedadnovin',"yearmonth":140009,"filePath":config.c2cbet_src_path+"140009-eghtedadnovin.xlsx"}]
    # f2db = FraudRealData2DB(db_props=config.derby_props)
    f2db = FraudRealData2DB(db_props=config.orcl_10_props)
    for f in inputs :
        f2db.etl(f["provider"],f["yearmonth"],f["filePath"])
    # f.etl('sadad',140008,config.c2cbet_src_path+"140008-sadad.xlsx")
    # f.etl('sadad',140009,config.c2cbet_src_path+"140009-sadad.xlsx")
    # f.etl('eghtedadnovin',140009,config.c2cbet_src_path+"140009-eghtedadnovin.xlsx")
