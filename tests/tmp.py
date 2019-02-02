import importlib
import d6tstack.utils
importlib.reload(d6tstack.utils)

import time
import yaml

config = yaml.load(open('tests/.test-cred.yaml'))

cfg_uri_psql = config['rds']
cfg_uri_psql = config['wlo']

import pandas as pd
df = pd.DataFrame({'a':range(10),'b':range(10)})
d6tstack.utils.pd_to_psql(df,cfg_uri_psql,'quick',sep='\t',if_exists='replace')
print(pd.read_sql_table('quick',sqlengine))



import yaml
config = yaml.load(open('.test-cred.yaml'))
cfg_uri_psql = config['wlo']

import pandas as pd
df = pd.DataFrame({'a':range(10),'b':range(10),'name':['name,first name']*10})

import d6tstack.utils
d6tstack.utils.pd_to_psql(df,cfg_uri_psql,'quick',sep='\t',if_exists='replace')

import sqlalchemy
sqlengine = sqlalchemy.create_engine(cfg_uri_psql)
print(pd.read_sql_table('quick',sqlengine))





config = yaml.load(open('tests/.test-cred.yaml'))
cfg_uri_mysql = config['local-mysql']
sqlengine = sqlalchemy.create_engine(cfg_uri_mysql)
importlib.reload(d6tstack.utils)
d6tstack.utils.pd_to_mysql(df,cfg_uri_mysql,'quick',if_exists='replace')
print(pd.read_sql_table('quick',sqlengine))


import sqlalchemy
sqlengine = sqlalchemy.create_engine(cfg_uri_psql)
sqlengine = sqlalchemy.create_engine(cfg_uri_mysql)

sqlengine = sqlalchemy.create_engine(cfg_uri_psql)
print(pd.read_sql_table('benchmark',sqlengine).head())

dft = pd.read_sql_table('benchmark',sqlengine)
dft.shape

# cursor = sqlengine.cursor()
sql = sqlengine.execute("SELECT * FROM benchmark;")
dft2 = pd.DataFrame(sql.fetchall())
dft2.shape
sql.keys()

importlib.reload(d6tstack.utils)

start_time = time.time()
dft2 = d6tstack.utils.pd_from_sqlengine(cfg_uri_psql, "SELECT * FROM benchmark;")
assert dft2.shape==(100000, 23)
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
dft = pd.read_sql_table('benchmark',sqlengine)
assert dft.shape==(100000, 23)
print("--- %s seconds ---" % (time.time() - start_time))

d6tstack.utils.test()

