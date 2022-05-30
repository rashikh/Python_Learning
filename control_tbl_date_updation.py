# Purpose: To update the date and time stamp of control table in one cluster of Redshift taken from date and timestamp from different cluster.
# sample code
"""

import psycopg2
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import configparser
import os
import sys
config = configparser.RawConfigParser()
config.read(os.path.join(os.path.abspath(__file__+ "/../../../") , "config" , "config.ini"))
dbname_r = config['REDSHIFT_01']['dbname']
host_r = config['REDSHIFT_01']['host']
port_r = config['REDSHIFT_01']['port']
user_r = config['REDSHIFT_01']['user']
password_r =util.loadProperties(os.path.join(os.path.abspath(__file__+ "/../../../") , "config" , "config.ini"),'REDSHIFT_01')['password_enc']
print(os.path.join(os.path.abspath(__file__+ "/../../../") , "config" , "config.ini"))

rds_connect_str = 'dbname=' + dbname_r + ' host=' + host_r + ' port=' + port_r + ' user=' + user_r + ' password=' + password_r
con1 = psycopg2.connect(rds_connect_str)



print('connected successfully connection1')

cur = con1.cursor()

cur.execute("SELECT max(updt_dttm)  FROM Tab1")
updt_dttm_us = np.array(cur.fetchall())
cur.close()
con1.close()
distinct_date = pd.DataFrame(updt_dttm_cls1)
print(distinct_date)
dbname = config['REDSHIFT_02']['dbname']
host = config['REDSHIFT_02']['host']
port = config['REDSHIFT_02']['port']
user = config['REDSHIFT_02']['user']
password=util.loadProperties(os.path.join(os.path.abspath(__file__+ "/../../../") , "config" , "config.ini"),'REDSHIFT_02')['password_enc']
print(os.path.join(os.path.abspath(__file__+ "/../../../") , "config" , "config.ini"))
rds_connect_str1 = 'postgres://' + user + ':' + password + '@' + host + ':' + port + '/' + dbname
print(rds_connect_str1)
con2 = create_engine (rds_connect_str1)

print('connected successfully connection2')
query = "select * from contr_tab1_clus1 where source_table='rab1';"
df2 = pd.read_sql(query, con2)
df2['updt_dttm'] = distinct_date
print(df2)

delete_query2 = " Delete from contr_tab1_clus1 where  source_table='tab1'"
con2.execute(delete_query2)
df2.to_sql('cont_tab1', schema='cont_tab1_schema', con=con2, index=False, if_exists='append')