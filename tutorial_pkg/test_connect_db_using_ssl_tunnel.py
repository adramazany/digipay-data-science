""" test_connect_db_using_ssl_tunnel :
    4/24/2022 5:47 PM
    ...
"""
__author__ = "Adel Ramezani <adramazany@gmail.com>"

from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder #Run pip install sshtunnel

server = SSHTunnelForwarder(("10.198.31.51", 22),
    ssh_username="oracle",ssh_password="D!G!P@Y",
    remote_bind_address=("10.198.31.35", 1433))
         # ssh_pkey=<'path/to/key.pem'>,  # or ssh_password.

server.start()

# "mssql+pyodbc://data-team:data-team@10.198.31.35"
# "mssql+pymssql://data-team:data-team@10.198.31.35/DGPAY_3G"
db_url = "mssql+pymssql://{user}:{password}@{host}:{port}/{db}".format(
    user="data-team",password="data-team",
    host="127.0.0.1",port=server.local_bind_port,db="DGPAY_3G")
    # host=server.local_bind_host,port=server.local_bind_port,db="DGPAY_3G")

engine = create_engine(db_url)

cnt = engine.execute("select count(*) from GL").scalar()
print(f"cnt={cnt}")


server.stop()