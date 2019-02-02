# -*- coding: utf-8 -*-
"""
Created on Sun Jun 24 10:45:14 2018

@author: deepmind
"""

import pandas as pd
import glob
from fastparquet import write
from fastparquet import ParquetFile

import pyarrow.parquet as pq

for fname in glob.glob('test-data-input-csv-*.csv'):
    df=pd.read_csv(fname)
    df['date']=pd.to_datetime(df['date'],format='%Y-%m-%d')
#    write(fname[:-4]+'.parq', df)
    pq.write_table(table, 'example.parquet')pa.Table.from_pandas(df)

import dask.dataframe as dd
ddf = dd.read_parquet('test-data-input-csv-*.csv')
ddf.head()

ddf = dd.read_parquet('test-data-input-csv-*.parq')
ddf.head()
ddf.tail()
ddf.compute()

dft = ParquetFile('test-data-input-csv-mar.parq').to_pandas()
assert df.equals(dft)

ddf = dd.read_csv('test-data-input-csv-*.parq')
