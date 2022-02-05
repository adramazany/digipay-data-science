import logging

from sqlalchemy import create_engine
import cx_Oracle
import time
# import os
# logger.info('environ=',os.environ)

cx_Oracle.init_oracle_client('d:/app/oracle_instantclient_19_10')
# logging.basicConfig(level=logging.INFO ,format='%(asctime)s %(clientip)-15s %(user)-8s %(message)s')
logging.basicConfig(level=logging.INFO ,format='%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

t1=time.time()

logger.info("create engine ....")
engine = create_engine("oracle+cx_oracle://mongodb:Mongo123@172.18.24.84:1521/?service_name=ORCL")
sql = "select P_DATE from dim_date order by DATE_ID"

logger.info("connecting ....")
counter=0
with engine.connect() as conn:
    logger.info("connecting ....")
    rs = conn.execute(sql)
    while True:
        logger.info("fetching ....")
        chunk = rs.fetchmany(1000)
        if not chunk or counter>2:
              break
        for row in chunk:
            logger.info(row[0])
        counter+=1

logger.info("finished. duration=%s"%((time.time()-t1)))
