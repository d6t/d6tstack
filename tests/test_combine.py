"""Run unit tests.

Use this to run tests and understand how tasks.py works.

Example:

    Create directories::

        mkdir -p test-data/input
        mkdir -p test-data/output

    Run tests::

        pytest test_combine.py -s

Notes:

    * this will create sample csv, xls and xlsx files    
    * test_combine_() test the main combine function

"""

from d6t.stack.combine_files import *
from d6t.stack.combine_csv import *
from d6t.stack.combine_xls import *

import pandas as pd
import ntpath

import pytest

cfg_fname_base_in = 'test-data/input/test-data-'
cfg_fname_base_out_dir = 'test-data/output'
cfg_fname_base_out = cfg_fname_base_out_dir+'/test-data-'

#************************************************************
# fixtures
#************************************************************
class TestLogPusher(object):
    def __init__(self, event):
        pass
        
    def send_log(self, msg, status):
        pass

    def send(self, data):
        pass

logger = TestLogPusher('combiner')

# sample data
def create_files_df_clean():
    # create sample data
    df1=pd.DataFrame({'date':pd.date_range('1/1/2011', periods=10), 'sales': 100, 'cost':-80, 'profit':20})
    df2=pd.DataFrame({'date':pd.date_range('2/1/2011', periods=10), 'sales': 200, 'cost':-90, 'profit':200-90})
    df3=pd.DataFrame({'date':pd.date_range('3/1/2011', periods=10), 'sales': 300, 'cost':-100, 'profit':300-100})
#    cfg_col = [ 'date', 'sales','cost','profit']
    
    # return df1[cfg_col], df2[cfg_col], df3[cfg_col]
    return df1, df2, df3

def create_files_df_clean_combine():
    df1,df2,df3 = create_files_df_clean()
    df_all = pd.concat([df1,df2,df3])
    df_all = df_all[df_all.columns].astype(str)
    
    return df_all


def create_files_df_colmismatch_combine(cfg_col_common):
    df1, df2, df3 = create_files_df_clean()
    df3['profit2']=df3['profit']*2
    if cfg_col_common:
        df_all = pd.concat([df1, df2, df3], join='inner')
    else:
        df_all = pd.concat([df1, df2, df3])
    df_all = df_all[df_all.columns].astype(str)

    return df_all


def create_files_df_colmismatch_combine2(cfg_col_common):
    df1, df2, df3 = create_files_df_clean()
    for i in range(15):
        df3['profit'+str(i)]=df3['profit']*2
    if cfg_col_common:
        df_all = pd.concat([df1, df2, df3], join='inner')
    else:
        df_all = pd.concat([df1, df2, df3])
    df_all = df_all[df_all.columns].astype(str)

    return df_all


# csv standard
@pytest.fixture(scope="module")
def create_files_csv():

    df1,df2,df3 = create_files_df_clean()
    # save files
    cfg_fname = cfg_fname_base_in+'input-csv-clean-%s.csv'
    df1.to_csv(cfg_fname % 'jan',index=False)
    df2.to_csv(cfg_fname % 'feb',index=False)
    df3.to_csv(cfg_fname % 'mar',index=False)

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']

@pytest.fixture(scope="module")
def create_files_csv_colmismatch():

    df1,df2,df3 = create_files_df_clean()
    df3['profit2']=df3['profit']*2
    # save files
    cfg_fname = cfg_fname_base_in+'input-csv-colmismatch-%s.csv'
    df1.to_csv(cfg_fname % 'jan',index=False)
    df2.to_csv(cfg_fname % 'feb',index=False)
    df3.to_csv(cfg_fname % 'mar',index=False)

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']

@pytest.fixture(scope="module")
def create_files_csv_colmismatch2():

    df1,df2,df3 = create_files_df_clean()
    for i in range(15):
        df3['profit'+str(i)]=df3['profit']*2
    # save files
    cfg_fname = cfg_fname_base_in+'input-csv-colmismatch2-%s.csv'
    df1.to_csv(cfg_fname % 'jan',index=False)
    df2.to_csv(cfg_fname % 'feb',index=False)
    df3.to_csv(cfg_fname % 'mar',index=False)

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']

@pytest.fixture(scope="module")
def create_files_csv_colreorder():

    df1,df2,df3 = create_files_df_clean()
    cfg_col = [ 'date', 'sales','cost','profit']
    cfg_col2 = [ 'date', 'sales','profit','cost']
    
    # return df1[cfg_col], df2[cfg_col], df3[cfg_col]
    # save files
    cfg_fname = cfg_fname_base_in+'input-csv-reorder-%s.csv'
    df1[cfg_col].to_csv(cfg_fname % 'jan',index=False)
    df2[cfg_col].to_csv(cfg_fname % 'feb',index=False)
    df3[cfg_col2].to_csv(cfg_fname % 'mar',index=False)

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']

@pytest.fixture(scope="module")
def create_files_csv_noheader():

    df1,df2,df3 = create_files_df_clean()
    # save files
    cfg_fname = cfg_fname_base_in+'input-noheader-csv-%s.csv'
    df1.to_csv(cfg_fname % 'jan',index=False, header=False)
    df2.to_csv(cfg_fname % 'feb',index=False, header=False)
    df3.to_csv(cfg_fname % 'mar',index=False, header=False)

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']

def create_files_csv_dirty(cfg_sep=",", cfg_header=True):

    df1,df2,df3 = create_files_df_clean()
    df1.to_csv(cfg_fname_base_in+'debug.csv',index=False, sep=cfg_sep, header=cfg_header)

    return cfg_fname_base_in+'debug.csv'

# excel single-tab
def create_files_xls_single_helper(cfg_fname):
    df1,df2,df3 = create_files_df_clean()
    df1.to_excel(cfg_fname % 'jan',index=False)
    df2.to_excel(cfg_fname % 'feb',index=False)
    df3.to_excel(cfg_fname % 'mar',index=False)

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']

@pytest.fixture(scope="module")
def create_files_xls_single():
    return create_files_xls_single_helper(cfg_fname_base_in+'input-xls-sing-%s.xls')

@pytest.fixture(scope="module")
def create_files_xlsx_single():
    return create_files_xls_single_helper(cfg_fname_base_in+'input-xls-sing-%s.xlsx')

# excel multi-tab
def create_files_xls_multiple_helper(cfg_fname):
    def write_file(dfg,fname):
        writer = pd.ExcelWriter(fname)
        dfg.to_excel(writer,'Sheet1',index=False)
        dfg.to_excel(writer,'Sheet2',index=False)
        writer.save()

    df1,df2,df3 = create_files_df_clean()
    write_file(df1,cfg_fname % 'jan')
    write_file(df2,cfg_fname % 'feb')
    write_file(df3,cfg_fname % 'mar')

    return [cfg_fname % 'jan',cfg_fname % 'feb',cfg_fname % 'mar']
    
@pytest.fixture(scope="module")
def create_files_xls_multiple():
    return create_files_xls_multiple_helper(cfg_fname_base_in+'input-xls-mult-%s.xls')

@pytest.fixture(scope="module")
def create_files_xlsx_multiple():
    return create_files_xls_multiple_helper(cfg_fname_base_in+'input-xls-mult-%s.xlsx')
    
#************************************************************
# tests - helpers
#************************************************************

def test_file_extensions_get():
    fname_list = ['a.csv','b.csv']
    ext_list = file_extensions_get(fname_list)
    assert ext_list==['.csv','.csv']
    
    fname_list = ['a.xls','b.xls']
    ext_list = file_extensions_get(fname_list)
    assert ext_list==['.xls','.xls']

def test_file_extensions_all_equal():
    ext_list = ['.csv']*2
    assert file_extensions_all_equal(ext_list)
    ext_list = ['.xls']*2
    assert file_extensions_all_equal(ext_list)
    ext_list = ['.csv','.xls']
    assert not file_extensions_all_equal(ext_list)
    
def test_file_extensions_valid():
    ext_list = ['.csv']*2
    assert file_extensions_valid(ext_list)
    ext_list = ['.xls']*2
    assert file_extensions_valid(ext_list)
    ext_list = ['.exe','.xls']
    assert not file_extensions_valid(ext_list)

#************************************************************
#************************************************************
# combine_csv
#************************************************************
#************************************************************
def test_csv_sniff_single(create_files_csv, create_files_csv_noheader):
    sniff = CSVSniffer(create_files_csv[0])
    sniff.get_delim()
    assert sniff.delim == ','
    assert sniff.count_skiprows() == 0
    assert sniff.has_header()

    fname = create_files_csv_dirty("|")
    sniff = CSVSniffer(fname)
    sniff.get_delim()
    assert sniff.delim == "|"
    assert sniff.has_header()

    df1,df2,df3 = create_files_df_clean()
    assert sniff.nrows == df1.shape[0]+1

    # no header test
    sniff = CSVSniffer(create_files_csv_noheader[0])
    sniff.get_delim()
    assert sniff.delim == ','
    assert sniff.count_skiprows() == 0
    assert not sniff.has_header()

def test_csv_sniff_multi(create_files_csv, create_files_csv_noheader):
    sniff = CSVSnifferList(create_files_csv)
    assert sniff.get_delim() == ','
    assert sniff.count_skiprows() == 0
    assert sniff.has_header()

    # no header test
    sniff = CSVSnifferList(create_files_csv_noheader)
    sniff.get_delim()
    assert sniff.get_delim() == ','
    assert sniff.count_skiprows() == 0
    assert not sniff.has_header()


def test_CombinerCSV_columns(create_files_csv, create_files_csv_colmismatch, create_files_csv_colreorder):

    fname_list = create_files_csv
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    col_preview = combiner.preview_columns()
    # todo: cache the preview dfs somehow? reading the same in next step
    
    assert col_preview['is_all_equal']
    assert col_preview['columns_all']==col_preview['columns_common']
    assert col_preview['columns_all']==['cost', 'date', 'profit', 'sales']

    fname_list = create_files_csv_colmismatch
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    col_preview = combiner.preview_columns()
    # todo: cache the preview dfs somehow? reading the same in next step
    
    assert not col_preview['is_all_equal']
    assert not col_preview['columns_all']==col_preview['columns_common']
    assert col_preview['columns_all']==['cost', 'date', 'profit', 'profit2', 'sales']
    assert col_preview['columns_common']==['cost', 'date', 'profit', 'sales']
    assert col_preview['columns_unique']==['profit2']

    fname_list = create_files_csv_colreorder
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    col_preview = combiner.preview_columns()

    assert not col_preview['is_all_equal']
    assert col_preview['columns_all']==col_preview['columns_common']


def test_CombinerCSV_combine(create_files_csv, create_files_csv_colmismatch, create_files_csv_colreorder):

    # all columns present
    fname_list = create_files_csv
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    df = combiner.combine()

    df = df.sort_values('date').drop(['filename'],axis=1)
    df_chk = create_files_df_clean_combine()
    assert df.equals(df_chk)

    df = combiner.combine()
    df = df.groupby('filename').head(combiner.nrows_preview)
    df_chk = combiner.combine_preview()
    assert df.equals(df_chk)

    # columns mismatch, all columns
    fname_list = create_files_csv_colmismatch
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    df = combiner.combine()
    df = df.sort_values('date').drop(['filename'],axis=1)
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=False)
    assert df.shape[1] == df_chk.shape[1]

    # columns mismatch, common columns
    df = combiner.combine(is_col_common=True)
    df = df.sort_values('date').drop(['filename'], axis=1)
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=True)
    assert df.shape[1] == df_chk.shape[1]


def test_CombinerCSVAdvanced_combine(create_files_csv):

    # Check if rename worked correctly.
    fname_list = create_files_csv
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    adv_combiner = CombinerCSVAdvanced(combiner, cfg_col_sel=None, cfg_col_rename={'date':'date1'})

    df = adv_combiner.combine()
    assert 'date1' in df.columns.values
    assert 'date' not in df.columns.values

    df = adv_combiner.combine_preview()
    assert 'date1' in df.columns.values
    assert 'date' not in df.columns.values

    adv_combiner = CombinerCSVAdvanced(combiner, cfg_col_sel=['cost', 'date', 'profit', 'profit2', 'sales'])

    df = adv_combiner.combine()
    assert 'profit2' in df.columns.values
    assert df['profit2'].isnull().all()

    df = adv_combiner.combine_preview()
    assert 'profit2' in df.columns.values
    assert df['profit2'].isnull().all()

#************************************************************
# combine_xls
#************************************************************

def test_xls_scan_sheets_single(create_files_xls_single,create_files_xlsx_single):
    def helper(fnames):
        xlsSniffer = XLSSniffer(fnames)
        sheets = xlsSniffer.dict_xls_sheets
        assert np.all([file['sheets_names']==['Sheet1'] for file in sheets.values()])
        assert np.all([file['sheets_count']==1 for file in sheets.values()])
        assert xlsSniffer.all_same_count()
        assert xlsSniffer.all_same_names()
        assert xlsSniffer.all_contain_sheetname('Sheet1')
        assert xlsSniffer.all_have_idx(0)
        assert not xlsSniffer.all_have_idx(1)
    
    helper(create_files_xls_single)
    helper(create_files_xlsx_single)

def test_xls_scan_sheets_multipe(create_files_xls_multiple,create_files_xlsx_multiple):
    def helper(fnames):
        xlsSniffer = XLSSniffer(fnames)
        sheets = xlsSniffer.dict_xls_sheets
        assert np.all([file['sheets_names']==['Sheet1', 'Sheet2'] for file in sheets.values()])
        assert np.all([file['sheets_count']==2 for file in sheets.values()])

    helper(create_files_xls_multiple)
    helper(create_files_xlsx_multiple)

#todo: wrong file raises exception NotImplementedError


#************************************************************
# tests - ui
#************************************************************
def test_combine_csv(create_files_csv):
    r = combine_files(create_files_csv, '', logger, cfg_return_df=True)
    assert r['status']=='complete'
    df = r['data']
    df2 = r['data'].copy().reset_index(drop=True)
    df = df.sort_values('date').drop(['filename'],axis=1)
    df_chk = create_files_df_clean_combine()
    assert df.equals(df_chk)

    r = combine_files(create_files_csv, cfg_fname_base_out_dir, logger, cfg_return_df=False)
    assert r['status']=='complete'
    df = pd.read_csv(cfg_fname_base_out_dir+'/combined.csv',dtype=str)
    assert df.equals(df2)
    df_sample = pd.read_csv(cfg_fname_base_out_dir+'/combined-sample.csv',dtype=str)
    assert df_sample.equals(df2.groupby('filename').head(5).set_index('filename').reset_index())


def test_combine_csv_colmismatch(create_files_csv_colmismatch):
    r = combine_files(create_files_csv_colmismatch, '', logger, cfg_return_df=True)
    assert r['status']=='need_columns'
    
    r['settings']['columns_select_mode'] = 'all'
    r = combine_files(create_files_csv_colmismatch, '', logger, cfg_settings = r['settings'], cfg_return_df=True)
    assert r['status']=='complete'
    assert r['settings']['columns_select_mode'] == 'all'
    df = r['data']
    df = df.sort_values('date').drop(['filename'],axis=1)
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=False)
    assert df.shape[1] == df_chk.shape[1]

    # use common columns
    r['settings']['columns_select_mode'] = 'common'
    r = combine_files(create_files_csv_colmismatch, '', logger, cfg_settings = r['settings'], cfg_return_df=True)
    assert r['status'] == 'complete'
    df = r['data']
    df = df.sort_values('date').drop(['filename'], axis=1)
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=True)
    assert df.shape[1] == df_chk.shape[1]

def test_combine_csv_colmismatch2(create_files_csv_colmismatch2):
    r = combine_files(create_files_csv_colmismatch2, '', logger, cfg_return_df=True)
    assert r['status']=='need_columns'
    assert set([d['fname'] for d in r['columns_files']])==set([ntpath.basename(f) for f in create_files_csv_colmismatch2])
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=True)
    assert set([d['fname'] for d in r['columns_files']])==set([ntpath.basename(f) for f in create_files_csv_colmismatch2])
    assert [d['col_common_count'] for d in r['columns_files']]==[df_chk.shape[1]]*len(create_files_csv_colmismatch2)
    assert r['columns_files_all']['col_common_count'] == df_chk.shape[1]
    #todo: write more tests for r['columns_files']:['fname', 'col_common_names', 'col_common_count', 'col_unique_names', 'col_unique_count']

    # use all columns
    r['settings']['columns_select_mode'] = 'all'
    r = combine_files(create_files_csv_colmismatch2, '', logger, cfg_settings=r['settings'], cfg_return_df=True)
    assert r['status']=='complete'
    df = r['data']
    df = df.sort_values('date').drop(['filename'],axis=1)
    df_chk = create_files_df_colmismatch_combine2(cfg_col_common=False)
    assert df.shape[1] == df_chk.shape[1]

    # use common columns
    r['settings']['columns_select_mode'] = 'common'
    r = combine_files(create_files_csv_colmismatch2, '', logger, cfg_settings = r['settings'], cfg_return_df=True)
    assert r['status'] == 'complete'
    df = r['data']
    df = df.sort_values('date').drop(['filename'], axis=1)
    df_chk = create_files_df_colmismatch_combine2(cfg_col_common=True)
    assert df.shape[1] == df_chk.shape[1]


def helper_test_combine_xls_process(r):
    df = r['data']
    df = df.sort_values('date').drop(['filename'],axis=1)
    df['date'] = df['date'].str[0:10]
    df_chk = create_files_df_clean_combine()
    return df,df_chk

def test_combine_xls_single(create_files_xls_single):
    r = combine_files(create_files_xls_single, '', logger,cfg_return_df=True)
    df,df_chk = helper_test_combine_xls_process(r)

    assert df.equals(df_chk)

def test_combine_xls_multipe(create_files_xls_multiple):
    # run without input
    settings = {}
    r = combine_files(create_files_xls_multiple, '', logger, cfg_settings = settings, cfg_return_df=True)
    assert not r['status']=='complete'
    assert r['status']=='need_xls_sheets'

    # select sheets by name
    settings['xls_sheets_sel'] = dict(zip(create_files_xls_multiple,['Sheet1']*len(create_files_xls_multiple)))
    settings['xls_sheets_sel_mode'] = 'name'

    r = combine_files(create_files_xls_multiple, '', logger, cfg_settings = settings, cfg_return_df=True)
    df,df_chk = helper_test_combine_xls_process(r)

    assert df.equals(df_chk)

    # select sheets by index
    settings['xls_sheets_sel'] = dict(zip(create_files_xls_multiple,[0]*len(create_files_xls_multiple)))
    settings['xls_sheets_sel_mode'] = 'idx'

    r = combine_files(create_files_xls_multiple, '', logger, cfg_settings = settings, cfg_return_df=True)
    df,df_chk = helper_test_combine_xls_process(r)

    assert df.equals(df_chk)

def test_preview_dict():
    df = pd.DataFrame({'col1':[0,1],'col2':[0,1]})
    assert preview_dict(df) == {'columns': ['col1', 'col2'], 'rows': {0: [[0]], 1: [[1]]}}

