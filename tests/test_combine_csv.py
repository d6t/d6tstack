"""Run unit tests.

Use this to run tests and understand how tasks.py works.

Setup::

    mkdir -p test-data/input
    mkdir -p test-data/output
    mysql -u root -p
        CREATE DATABASE testdb;
        CREATE USER 'testusr'@'localhost' IDENTIFIED BY 'testpwd';
        GRANT ALL PRIVILEGES ON testdb.* TO 'testusr'@'%';

Run tests::

    pytest test_combine.py -s

Notes:

    * this will create sample csv, xls and xlsx files    
    * test_combine_() test the main combine function

"""

from d6tstack.combine_csv import *
from d6tstack.sniffer import CSVSniffer
import d6tstack.utils

import math
import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
import ntpath
import shutil
import dask.dataframe as dd
import sqlalchemy

import pytest

cfg_fname_base_in = 'test-data/input/test-data-'
cfg_fname_base_out_dir = 'test-data/output'
cfg_fname_base_out = cfg_fname_base_out_dir+'/test-data-'
cnxn_string = 'sqlite:///test-data/db/{}.db'

#************************************************************
# fixtures
#************************************************************
class DebugLogger(object):
    def __init__(self, event):
        pass
        
    def send_log(self, msg, status):
        pass

    def send(self, data):
        pass

logger = DebugLogger('combiner')

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


def create_files_df_clean_combine_with_filename(fname_list):
    df1, df2, df3 = create_files_df_clean()
    df1['filename'] = os.path.basename(fname_list[0])
    df2['filename'] = os.path.basename(fname_list[1])
    df3['filename'] = os.path.basename(fname_list[2])
    df_all = pd.concat([df1, df2, df3])
    df_all = df_all[df_all.columns].astype(str)

    return df_all


def create_files_df_colmismatch_combine(cfg_col_common,allstr=True):
    df1, df2, df3 = create_files_df_clean()
    df3['profit2']=df3['profit']*2
    if cfg_col_common:
        df_all = pd.concat([df1, df2, df3], join='inner')
    else:
        df_all = pd.concat([df1, df2, df3])
    if allstr:
        df_all = df_all[df_all.columns].astype(str)

    return df_all


def check_df_colmismatch_combine(dfg,is_common=False, convert_date=True):
    dfg = dfg.drop(['filepath','filename'],1).sort_values('date').reset_index(drop=True)
    if convert_date:
        dfg['date'] = pd.to_datetime(dfg['date'], format='%Y-%m-%d')
    dfchk = create_files_df_colmismatch_combine(is_common,False).reset_index(drop=True)[dfg.columns]
    assert dfg.equals(dfchk)
    return True


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

@pytest.fixture(scope="module")
def create_files_csv_col_renamed():

    df1, df2, df3 = create_files_df_clean()
    df3 = df3.rename(columns={'sales':'revenue'})
    cfg_col = ['date', 'sales', 'profit', 'cost']
    cfg_col2 = ['date', 'revenue', 'profit', 'cost']

    cfg_fname = cfg_fname_base_in + 'input-csv-renamed-%s.csv'
    df1[cfg_col].to_csv(cfg_fname % 'jan', index=False)
    df2[cfg_col].to_csv(cfg_fname % 'feb', index=False)
    df3[cfg_col2].to_csv(cfg_fname % 'mar', index=False)

    return [cfg_fname % 'jan', cfg_fname % 'feb', cfg_fname % 'mar']


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


def write_file_xls(dfg, fname, startrow=0,startcol=0):
    writer = pd.ExcelWriter(fname)
    dfg.to_excel(writer, 'Sheet1', index=False,startrow=startrow,startcol=startcol)
    dfg.to_excel(writer, 'Sheet2', index=False,startrow=startrow,startcol=startcol)
    writer.save()

# excel multi-tab
def create_files_xls_multiple_helper(cfg_fname):

    df1,df2,df3 = create_files_df_clean()
    write_file_xls(df1,cfg_fname % 'jan')
    write_file_xls(df2,cfg_fname % 'feb')
    write_file_xls(df3,cfg_fname % 'mar')

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
# scan header
#************************************************************
#************************************************************
def test_csv_sniff(create_files_csv, create_files_csv_colmismatch, create_files_csv_colreorder):

    with pytest.raises(ValueError) as e:
        c = CombinerCSV([])

    # clean
    combiner = CombinerCSV(fname_list=create_files_csv)
    combiner.sniff_columns()
    assert combiner.is_all_equal()
    assert combiner.is_column_present().all().all()
    assert combiner.sniff_results['columns_all'] == ['date', 'sales', 'cost', 'profit']
    assert combiner.sniff_results['columns_common'] == combiner.sniff_results['columns_all']
    assert combiner.sniff_results['columns_unique'] == []

    # extra column
    combiner = CombinerCSV(fname_list=create_files_csv_colmismatch)
    combiner.sniff_columns()
    assert not combiner.is_all_equal()
    assert not combiner.is_column_present().all().all()
    assert combiner.is_column_present().all().values.tolist()==[True, True, True, True, False]
    assert combiner.sniff_results['columns_all'] == ['date', 'sales', 'cost', 'profit', 'profit2']
    assert combiner.sniff_results['columns_common'] == ['date', 'sales', 'cost', 'profit']
    assert combiner.is_column_present_common().columns.tolist() == ['date', 'sales', 'cost', 'profit']
    assert combiner.sniff_results['columns_unique'] == ['profit2']
    assert combiner.is_column_present_unique().columns.tolist() == ['profit2']

    # mixed order
    combiner = CombinerCSV(fname_list=create_files_csv_colreorder)
    combiner.sniff_columns()
    assert not combiner.is_all_equal()
    assert combiner.sniff_results['df_columns_order']['profit'].values.tolist() == [3, 3, 2]


def test_csv_selectrename(create_files_csv, create_files_csv_colmismatch):

    # rename
    df = CombinerCSV(fname_list=create_files_csv).preview_rename()
    assert df.empty
    df = CombinerCSV(fname_list=create_files_csv, columns_rename={'notthere':'nan'}).preview_rename()
    assert df.empty

    df = CombinerCSV(fname_list=create_files_csv, columns_rename={'cost':'cost2'}).preview_rename()
    assert df.columns.tolist()==['cost']
    assert df['cost'].unique().tolist()==['cost2']

    df = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_rename={'profit2':'profit3'}).preview_rename()
    assert df.columns.tolist()==['profit2']
    assert df['profit2'].unique().tolist()==[np.nan, 'profit3']

    # select
    l = CombinerCSV(fname_list=create_files_csv).preview_select()
    assert l == ['date', 'sales', 'cost', 'profit']
    l2 = CombinerCSV(fname_list=create_files_csv, columns_select_common=True).preview_select()
    assert l2==l
    l = CombinerCSV(fname_list=create_files_csv, columns_select=['date', 'sales', 'cost']).preview_select()
    assert l == ['date', 'sales', 'cost']

    l = CombinerCSV(fname_list=create_files_csv_colmismatch).preview_select()
    assert l == ['date', 'sales', 'cost', 'profit', 'profit2']
    l = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select_common=True).preview_select()
    assert l == ['date', 'sales', 'cost', 'profit']

    # rename+select
    l = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select=['date','profit2'], columns_rename={'profit2':'profit3'}).preview_select()
    assert l==['date', 'profit3']
    l = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select=['date','profit3'], columns_rename={'profit2':'profit3'}).preview_select()
    assert l==['date', 'profit3']

def test_to_pandas(create_files_csv, create_files_csv_colmismatch, create_files_csv_colreorder):
    df = CombinerCSV(fname_list=create_files_csv).to_pandas()
    assert df.shape == (30, 6)
    
    df = CombinerCSV(fname_list=create_files_csv_colmismatch).to_pandas()
    assert df.shape == (30, 6+1)
    assert df['profit2'].isnull().unique().tolist() == [True, False]
    df = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select_common=True).to_pandas()
    assert df.shape == (30, 6)
    assert 'profit2' not in df.columns

    # rename+select
    df = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select=['date','profit2'], columns_rename={'profit2':'profit3'}, add_filename=False).to_pandas()
    assert df.shape == (30, 2)
    assert 'profit3' in df.columns and not 'profit2' in df.columns
    df = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select=['date','profit3'], columns_rename={'profit2':'profit3'}, add_filename=False).to_pandas()
    assert df.shape == (30, 2)
    assert 'profit3' in df.columns and not 'profit2' in df.columns

def test_combinepreview(create_files_csv_colmismatch):
    df = CombinerCSV(fname_list=create_files_csv_colmismatch).combine_preview()
    assert df.shape == (9, 6+1)
    assert df.dtypes.tolist() == [np.dtype('O'), np.dtype('int64'), np.dtype('int64'), np.dtype('int64'), np.dtype('float64'), np.dtype('O'), np.dtype('O')]

    def apply(dfg):
        dfg['date'] = pd.to_datetime(dfg['date'], format='%Y-%m-%d')
        return dfg

    df = CombinerCSV(fname_list=create_files_csv_colmismatch, apply_after_read=apply).combine_preview()
    assert df.shape == (9, 6+1)
    assert df.dtypes.tolist() == [np.dtype('<M8[ns]'), np.dtype('int64'), np.dtype('int64'), np.dtype('int64'), np.dtype('float64'), np.dtype('O'), np.dtype('O')]


def test_tocsv(create_files_csv_colmismatch):
    fname = 'test-data/output/combined.csv'
    fnameout = CombinerCSV(fname_list=create_files_csv_colmismatch).to_csv_combine(filename=fname)
    assert fname == fnameout
    df = pd.read_csv(fname)
    dfchk = df.copy()
    assert df.shape == (30, 4+1+2)
    assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    assert check_df_colmismatch_combine(df)
    fnameout = CombinerCSV(fname_list=create_files_csv_colmismatch, columns_select_common=True).to_csv_combine(filename=fname)
    df = pd.read_csv(fname)
    assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'filepath', 'filename']
    assert check_df_colmismatch_combine(df,is_common=True)

    def helper(fdir):
        fnamesout = CombinerCSV(fname_list=create_files_csv_colmismatch).to_csv_align(output_dir=fdir)
        for fname in fnamesout:
            df = pd.read_csv(fname)
            assert df.shape == (10, 4+1+2)
            assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    helper('test-data/output')
    helper('test-data/output/')

    df = dd.read_csv('test-data/output/d6tstack-test-data-input-csv-colmismatch-*.csv')
    df = df.compute()
    assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    assert df.reset_index(drop=True).equals(dfchk)
    assert check_df_colmismatch_combine(df)

    # check creates directory
    try:
        shutil.rmtree('test-data/output-tmp')
    except:
        pass
    _ = CombinerCSV(fname_list=create_files_csv_colmismatch).to_csv_align(output_dir='test-data/output-tmp')
    try:
        shutil.rmtree('test-data/output-tmp')
    except:
        pass


def test_topq(create_files_csv_colmismatch):
    fname = 'test-data/output/combined.pq'
    fnameout = CombinerCSV(fname_list=create_files_csv_colmismatch).to_parquet_combine(filename=fname)
    assert fname == fnameout
    df = pd.read_parquet(fname, engine='fastparquet')
    assert df.shape == (30, 4+1+2)
    assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    df2 = pd.read_parquet(fname, engine='pyarrow')
    assert df2.equals(df)
    assert check_df_colmismatch_combine(df)

    df = dd.read_parquet(fname)
    df = df.compute()
    assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    df2 = pd.read_parquet(fname, engine='fastparquet')
    assert df2.equals(df)
    df3 = pd.read_parquet(fname, engine='pyarrow')
    assert df3.equals(df)
    assert check_df_colmismatch_combine(df)


    def helper(fdir):
        fnamesout = CombinerCSV(fname_list=create_files_csv_colmismatch).to_parquet_align(output_dir=fdir)
        for fname in fnamesout:
            df = pd.read_parquet(fname, engine='fastparquet')
            assert df.shape == (10, 4+1+2)
            assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    helper('test-data/output')

    df = dd.read_parquet('test-data/output/d6tstack-test-data-input-csv-colmismatch-*.pq')
    df = df.compute()
    assert df.columns.tolist() == ['date', 'sales', 'cost', 'profit', 'profit2', 'filepath', 'filename']
    assert check_df_colmismatch_combine(df)

    # todo: write tests such that compare to concat df not always repeat same code to test shape and columns

def test_tosql(create_files_csv_colmismatch):
    tblname = 'testd6tstack'

    def apply(dfg):
        dfg['date'] = pd.to_datetime(dfg['date'], format='%Y-%m-%d')
        return dfg

    def helper(uri):
        sql_engine = sqlalchemy.create_engine(uri)
        CombinerCSV(fname_list=create_files_csv_colmismatch).to_sql_combine(uri, tblname, 'replace')
        df = pd.read_sql_table(tblname, sql_engine)
        assert check_df_colmismatch_combine(df)

        # with date convert
        CombinerCSV(fname_list=create_files_csv_colmismatch, apply_after_read=apply).to_sql_combine(uri, tblname, 'replace')
        df = pd.read_sql_table(tblname, sql_engine)
        assert check_df_colmismatch_combine(df, convert_date=False)

    uri = 'postgresql+psycopg2://psqlusr:psqlpwdpsqlpwd@localhost/psqltest'
    helper(uri)
    uri = 'mysql+pymysql://testusr:testpwd@localhost/testdb'
    helper(uri)

    uri = 'postgresql+psycopg2://psqlusr:psqlpwdpsqlpwd@localhost/psqltest'
    sql_engine = sqlalchemy.create_engine(uri)
    CombinerCSV(fname_list=create_files_csv_colmismatch).to_psql_combine(uri, tblname, if_exists='replace')
    df = pd.read_sql_table(tblname, sql_engine)
    assert df.shape == (30, 4+1+2)
    assert check_df_colmismatch_combine(df)

    CombinerCSV(fname_list=create_files_csv_colmismatch, apply_after_read=apply).to_psql_combine(uri, tblname, if_exists='replace')
    df = pd.read_sql_table(tblname, sql_engine)
    assert check_df_colmismatch_combine(df, convert_date=False)

    uri = 'mysql+mysqlconnector://testusr:testpwd@localhost/testdb'
    sql_engine = sqlalchemy.create_engine(uri)
    CombinerCSV(fname_list=create_files_csv_colmismatch).to_mysql_combine(uri, tblname, if_exists='replace')
    df = pd.read_sql_table(tblname, sql_engine)
    assert df.shape == (30, 4+1+2)
    assert check_df_colmismatch_combine(df)

    # todo: mysql import makes NaNs 0s
    CombinerCSV(fname_list=create_files_csv_colmismatch, apply_after_read=apply).to_mysql_combine(uri, tblname, if_exists='replace')
    df = pd.read_sql_table(tblname, sql_engine)
    assert check_df_colmismatch_combine(df, convert_date=False)


def test_tosql_util(create_files_csv_colmismatch):
    tblname = 'testd6tstack'

    uri = 'postgresql+psycopg2://psqlusr:psqlpwdpsqlpwd@localhost/psqltest'
    sql_engine = sqlalchemy.create_engine(uri)
    dfc = CombinerCSV(fname_list=create_files_csv_colmismatch).to_pandas()

    # psql
    d6tstack.utils.pd_to_psql(dfc, uri, tblname, if_exists='replace')
    df = pd.read_sql_table(tblname, sql_engine)
    assert df.equals(dfc)

    uri = 'mysql+mysqlconnector://testusr:testpwd@localhost/testdb'
    sql_engine = sqlalchemy.create_engine(uri)
    d6tstack.utils.pd_to_mysql(dfc, uri, tblname, if_exists='replace')
    df = pd.read_sql_table(tblname, sql_engine)
    assert df.equals(dfc)
