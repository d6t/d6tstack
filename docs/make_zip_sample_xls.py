import zipfile
import glob
import os



import pandas as pd
import numpy as np
# generate fake data
cfg_tickers = ['AAP','M','SPLS']
cfg_ntickers = len(cfg_tickers)
cfg_ndates = 10
cfg_dates = pd.bdate_range('2018-01-01',periods=cfg_ndates).tolist()+pd.bdate_range('2018-02-01',periods=cfg_ndates).tolist()
cfg_nobs = cfg_ndates*2
dft = pd.DataFrame({'date':np.tile(cfg_dates,cfg_ntickers), 'ticker':np.repeat(cfg_tickers,cfg_nobs)})


#****************************************
# xls
#****************************************
def write_file_xls(dfg, fname, sheets, startrow=0,startcol=0):
    writer = pd.ExcelWriter(fname)
    for isheet in sheets:
        dft['data'] = np.random.normal(size=dfg.shape[0])
        dfg['xls_sheet'] = isheet
        dfg.to_excel(writer, isheet, index=False,startrow=startrow,startcol=startcol)
    writer.save()

# excel - bad case => d6tstack. Fake data
cfg_path_base = 'test-data/excel_adv_data/sample-xls-'
df = dft
np.random.seed(0)
write_file_xls(df, cfg_path_base+'case-simple.xlsx',['Sheet1'])
write_file_xls(df, cfg_path_base+'case-multisheet.xlsx',['Sheet1','Sheet2'])
write_file_xls(df, cfg_path_base+'case-multifile1.xlsx',['Sheet1'])
write_file_xls(df, cfg_path_base+'case-multifile2.xlsx',['Sheet1'])
write_file_xls(df, cfg_path_base+'case-badlayout1.xlsx',['Sheet1','Sheet2'],startrow=1,startcol=1)

ziphandle = zipfile.ZipFile('test-data-xls.zip', 'w')
for fname in  glob.glob(cfg_path_base+'*.xlsx'):
    ziphandle.write(fname)

ziphandle.write('test-data/output/__init__.py')
ziphandle.close()

