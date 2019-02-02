#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 11:36:43 2018

@author: deepmind
"""

import pandas as pd
from d6tstack.helpers import *
import ntpath
import numpy as np

df1=pd.DataFrame({'b':range(10),'a':range(10)})
df2=pd.DataFrame({'b':range(10),'a':range(10),'c':range(10),})

dfl_all_col = [df.columns.tolist() for df in [df1,df2]]
col_files = dict(zip(['1','2'], dfl_all_col))
col_common = list_common(list(col_files.values()))
col_all = list_unique(list(col_files.values()))
col_unique = list(set(col_all) - set(col_common))

set(df1.columns.tolist())


df1.reindex(columns=df1.columns[df1.columns.isin(col_common)])
df2.reindex(columns=df1.columns[df1.columns.isin(col_common)])
df1.reindex(columns=df1.columns[df1.columns.isin(col_common)].tolist()+col_unique, copy=False)
df2.reindex(columns=df1.columns[df1.columns.isin(col_common)].tolist()+col_unique, copy=False)
df2

pd.concat([df1,df2],ignore_index=True,sort=False)


# find index in column list so can check order is correct
df_col_present = {}
for iFileName, iFileCol in col_files.items():
    df_col_present[iFileName] = [ntpath.basename(iFileName), ] + [iCol in iFileCol for iCol in col_all]

df_col_present = pd.DataFrame(df_col_present, index=['filename'] + col_all).T
df_col_present.index.names = ['file_path']

# find index in column list so can check order is correct
df_col_order = {}
for iFileName, iFileCol in col_files.items():
    df_col_order[iFileName] = [ntpath.basename(iFileName), ] + [
        iFileCol.index(iCol) if iCol in iFileCol else np.nan for iCol in col_all]
df_col_order = pd.DataFrame(df_col_order, index=['filename'] + col_all).T

df_col_order['b'].value_counts()

from scipy.stats import mode
dft=df_col_order.set_index('filename')
m=mode(dft,axis=0)
dforder = pd.DataFrame({'o':m[0][0],'c':m[1][0]},index=dft.columns)
dforder = dforder.sort_values(['o','c'])
dforder['iscommon']=dforder.index.isin(col_common)
dforder
dforder.index.values.tolist()

df_col_order.set_index('filename').T.groupby(level=0).agg(lambda x: mode(x))







