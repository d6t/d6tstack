import pandas as pd
import numpy as np
import glob
import d6tstack.combine_csv
import importlib
importlib.reload(d6tstack.combine_csv)
import sqlalchemy

fname_list=glob.glob('test-data/input/test-data-input-csv-clean-*.csv')
fname_list=glob.glob('test-data/input/test-data-input-csv-reorder-*.csv')
fname_list=glob.glob('test-data/input/test-data-input-csv-colmismatch-*.csv')
# combiner = d6tstack.combine_csv.CombinerCSV(fname_list)
# combiner.is_column_present().all().values.tolist()
# combiner.is_column_present_common()
# combiner.sniff_results['df_columns_order']['profit'].values.tolist()

uri = 'mysql+mysqlconnector://augvest:augvest@localhost/augvest'
uri = 'mysql+pymysql://augvest:augvest@localhost/augvest'

tblname = 'testd6tstack'

def apply(dfg):
    dfg['date'] = pd.to_datetime(dfg['date'], format='%Y-%m-%d')
    return dfg

# importlib.reload(d6tstack.combine_csv)
# combiner = d6tstack.combine_csv.CombinerCSV(fname_list)
# fnamesout = d6tstack.combine_csv.CombinerCSV(fname_list=fname_list, apply_after_read=apply).to_mysql_combine(uri,tblname,'replace')
#
# sql_engine = sqlalchemy.create_engine(uri)
# df = pd.read_sql_table(tblname, sql_engine)
# df['profit2']

importlib.reload(d6tstack.combine_csv)
sql_engine = sqlalchemy.create_engine(uri)
d6tstack.combine_csv.CombinerCSV(fname_list=fname_list).to_sql_combine(uri, tblname, {'if_exists': 'replace'})
df = pd.read_sql_table(tblname, sql_engine)
assert check_df_colmismatch_combine(df)

# todo: mysql import makes NaNs 0s

importlib.reload(d6tstack.combine_csv)
combiner = d6tstack.combine_csv.CombinerCSV(fname_list)
fnamesout = d6tstack.combine_csv.CombinerCSV(fname_list=fname_list).to_parquet_align(output_dir='test-data/output')
# fnamesout

import dask.dataframe as dd

df = dd.read_parquet('test-data/output/d6tstack-test-data-input-csv-colmismatch-*.pq',index='__index_level_0__')
df = df.compute()

for fname in fnamesout:
    df = dd.read_parquet(fname)
    df = df.compute()
    print(fname, df.columns)

df = dd.read_parquet(fnamesout[0])
df = df.compute()
df


