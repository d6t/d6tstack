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

from d6tstack.combine_csv import *
from d6tstack.sniffer import CSVSniffer

import pandas as pd
import ntpath

import pytest

cfg_fname_base_in = 'test-data/input/test-data-'
cfg_fname_base_out_dir = 'test-data/output'
cfg_fname_base_out = cfg_fname_base_out_dir+'/test-data-'
cnxn_string = 'sqlite:///test-data/db/{}.db'

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


def create_files_df_clean_combine_with_filename(fname_list):
    df1, df2, df3 = create_files_df_clean()
    df1['filename'] = os.path.basename(fname_list[0])
    df2['filename'] = os.path.basename(fname_list[1])
    df3['filename'] = os.path.basename(fname_list[2])
    df_all = pd.concat([df1, df2, df3])
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

@pytest.fixture(scope="module")
def create_files_csv_col_renamed():

    df1, df2, df3 = create_files_df_clean()
    cfg_col = ['date', 'sales', 'profit', 'cost']
    cfg_col2 = ['date', 'sales', 'profit_renamed', 'cost']

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

    with pytest.raises(ValueError) as e:
        c = CombinerCSV([])

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
    df_chk = combiner.preview_combine()
    assert df.equals(df_chk)

    # columns mismatch, all columns
    fname_list = create_files_csv_colmismatch
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True, cfg_filename_col=True)
    df = combiner.combine()
    df = df.sort_values('date').drop(['filename'],axis=1)
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=False)
    assert df.shape[1] == df_chk.shape[1]

    # columns mismatch, common columns
    df = combiner.combine(is_col_common=True)
    df = df.sort_values('date').drop(['filename'], axis=1)
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=True)
    assert df.shape[1] == df_chk.shape[1]

    # Filename column True
    fname_list = create_files_csv
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    df = combiner.combine()

    df = df.sort_values('date')
    df_chk = create_files_df_clean_combine_with_filename(fname_list)
    assert df.equals(df_chk)

    # Filename column False
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True, cfg_filename_col=False)
    df = combiner.combine()
    df = df.sort_values('date')
    df_chk = create_files_df_clean_combine()
    assert df.equals(df_chk)


def test_CombinerCSVAdvanced_combine(create_files_csv):

    # Check if rename worked correctly.
    fname_list = create_files_csv
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True)
    adv_combiner = CombinerCSVAdvanced(combiner, cfg_col_sel=None, cfg_col_rename={'date':'date1'})

    df = adv_combiner.combine()
    assert 'date1' in df.columns.values
    assert 'date' not in df.columns.values

    df = adv_combiner.preview_combine()
    assert 'date1' in df.columns.values
    assert 'date' not in df.columns.values

    adv_combiner = CombinerCSVAdvanced(combiner, cfg_col_sel=['cost', 'date', 'profit', 'profit2', 'sales'])

    df = adv_combiner.combine()
    assert 'profit2' in df.columns.values
    assert df['profit2'].isnull().all()

    df = adv_combiner.preview_combine()
    assert 'profit2' in df.columns.values
    assert df['profit2'].isnull().all()


def test_preview_dict():
    df = pd.DataFrame({'col1':[0,1],'col2':[0,1]})
    assert preview_dict(df) == {'columns': ['col1', 'col2'], 'rows': {0: [[0]], 1: [[1]]}}


#************************************************************
# tests - CombinerCSVAdvanced rename
#************************************************************
def create_df_rename():
    df11 = pd.DataFrame({'a':range(10)})
    df12 = pd.DataFrame({'b': range(10)})
    df21 = pd.DataFrame({'a':range(10),'c': range(10)})
    df22 = pd.DataFrame({'b': range(10),'c': range(10)})

    return df11, df12, df21, df22

# csv standard
@pytest.fixture(scope="module")
def create_files_csv_rename():

    df11, df12, df21, df22 = create_df_rename()
    # save files
    cfg_fname = cfg_fname_base_in+'input-csv-rename-%s.csv'
    df11.to_csv(cfg_fname % '11',index=False)
    df12.to_csv(cfg_fname % '12',index=False)
    df21.to_csv(cfg_fname % '21',index=False)
    df22.to_csv(cfg_fname % '22',index=False)

    return [cfg_fname % '11',cfg_fname % '12',cfg_fname % '21',cfg_fname % '22']


@pytest.fixture(scope="module")
def create_out_files_csv_align_save():
    cfg_outname = cfg_fname_base_out + 'input-csv-rename-%s-align-save.csv'
    return [cfg_outname % '11', cfg_outname % '12',cfg_outname % '21',cfg_outname % '22']

def test_apply_select_rename():
    df11, df12, df21, df22 = create_df_rename()

    # rename 1, select all
    assert df11.equals(apply_select_rename(df12.copy(),[],{'b':'a'}))

    # rename and select 1
    assert df11.equals(apply_select_rename(df22.copy(),['b'],{'b':'a'}))
    assert df11.equals(apply_select_rename(df22.copy(),['a'],{'b':'a'}))

    # rename and select 2
    assert df21[list(set(df21.columns))].equals(apply_select_rename(df22.copy(),['b','c'],{'b':'a'}))
    assert df21[list(set(df21.columns))].equals(apply_select_rename(df22.copy(),['a','c'],{'b':'a'}))

    with pytest.raises(ValueError) as e:
        assert df21.equals(apply_select_rename(df22.copy(), ['b', 'c'], {'b': 'c'}))


def test_CombinerCSVAdvanced_rename(create_files_csv_rename):
    df11, df12, df21, df22 = create_df_rename()
    df_chk1 = pd.concat([df11,df11])
    df_chk2 = pd.concat([df11,df21])

    def helper(fnames, cfg_col_sel,cfg_col_rename, df_chk, chk_filename=False, is_filename_col=True):
        c = CombinerCSV(fnames, cfg_filename_col=is_filename_col)
        if cfg_col_sel and cfg_col_rename:
            c2 = CombinerCSVAdvanced(c, cfg_col_sel=cfg_col_sel, cfg_col_rename=cfg_col_rename)
        elif cfg_col_rename:
            c2 = CombinerCSVAdvanced(c, cfg_col_rename=cfg_col_rename)
        else:
            c2 = CombinerCSVAdvanced(c)

        dfc = c2.combine()
        if (not chk_filename) and is_filename_col:
            dfc = dfc.drop(['filename'], 1)
        assert dfc.equals(df_chk)

        if cfg_col_sel:
            fname_out = cfg_fname_base_out_dir + '/test_save.csv'
            c2.combine_save(fname_out)
            dfc = pd.read_csv(fname_out)
            if (not chk_filename) or (not is_filename_col):
                dfc = dfc.drop(['filename'], 1)
            assert dfc.equals(df_chk.reset_index(drop=True))

    # rename 1, select all
    l = create_files_csv_rename[:2]
    helper(l,None,{'b':'a'},df_chk1)

    with pytest.raises(ValueError) as e:
        c = CombinerCSV(l)
        c2 = CombinerCSVAdvanced(c, cfg_col_sel=['a','a'])

    # rename 2 col to same in same file
    # l = [create_files_csv_rename[2]]
    # helper(l, None, {'a': 'c'}, df_chk1)

    # rename 1, select some
    l = [create_files_csv_rename[0],create_files_csv_rename[-1]]
    helper(l,['a'],{'b':'a'},df_chk1)
    helper(l,['b'],{'b':'a'},df_chk1)
    helper(l,None,{'b':'a'},df_chk2)

    l = [create_files_csv_rename[1],create_files_csv_rename[-1]]
    helper(l,['a'],{'b':'a'},df_chk1)
    helper(l,['b'],{'b':'a'},df_chk1)
    helper(l,None,{'b':'a'},df_chk2)

    with pytest.raises(ValueError) as e:
        c = CombinerCSV(l)
        c2 = CombinerCSVAdvanced(c, cfg_col_rename={'b':'a','c':'a'})
        c2.combine()

    # rename none, select all
    l = [create_files_csv_rename[0],create_files_csv_rename[2]]
    helper(l,None,None,df_chk2)

    # filename col True
    df31 = df11
    df32 = df21
    df31['filename'] = os.path.basename(l[0])
    df32['filename'] = os.path.basename(l[1])
    df_chk3 = pd.concat([df31, df32])
    helper(l, None, None, df_chk3, is_filename_col=True, chk_filename=True)
    helper(l, None, None, df_chk2, is_filename_col=False, chk_filename=True)


def test_CombinerCSVAdvanced_align_save(create_files_csv_rename, create_out_files_csv_align_save):
    df11, df12, df21, df22 = create_df_rename()

    def helper(fnames, cfg_col_sel, cfg_col_rename, new_fnames, df_chks, is_filename_col=False):
        c = CombinerCSV(fnames, cfg_filename_col=is_filename_col)
        if cfg_col_sel and cfg_col_rename:
            c2 = CombinerCSVAdvanced(c, cfg_col_sel=cfg_col_sel, cfg_col_rename=cfg_col_rename)
        elif cfg_col_sel:
            c2 = CombinerCSVAdvanced(c, cfg_col_sel=cfg_col_sel)
        elif cfg_col_rename:
            c2 = CombinerCSVAdvanced(c, cfg_col_rename=cfg_col_rename)
        else:
            c2 = CombinerCSVAdvanced(c)
            
        c2.align_save(output_dir=cfg_fname_base_out_dir, suffix="-align-save")
        for fname_out, df_chk in zip(new_fnames, df_chks):
            dfc = pd.read_csv(fname_out)
            assert dfc.equals(df_chk)
        
    # rename 1, select all
    l = create_files_csv_rename[:2]
    outl = create_out_files_csv_align_save[:2]
    helper(l, ['a'], {'b':'a'}, outl, [df11, df11])

    with pytest.raises(ValueError) as e:
        c = CombinerCSV(l)
        c2 = CombinerCSVAdvanced(c)
        c2.align_save()

    # rename 1, select some
    l = [create_files_csv_rename[2]]
    outl = [create_out_files_csv_align_save[2]]
    helper(l, ['a'], {'b':'a'}, outl, [df11])

    # rename none, select 1
    l = [create_files_csv_rename[2]]
    outl = [create_out_files_csv_align_save[2]]
    helper(l, ['a'], None, outl, [df11])

    # rename none, select all
    l = [create_files_csv_rename[2]]
    outl = [create_out_files_csv_align_save[2]]
    helper(l, ['a', 'c'], None, outl, [df21])

    # rename none, select all, filename col true
    df21['filename'] = os.path.basename(outl[0])
    helper(l, ['a', 'c'], None, outl, [df21], is_filename_col=True)


def test_CombinerCSVAdvanced_sql(create_files_csv_rename):
    df11, df12, df21, df22 = create_df_rename()

    def helper(fnames, cfg_col_sel, cfg_col_rename, df_chks, is_filename_col=False, stream=False):
        c = CombinerCSV(fnames, cfg_filename_col=is_filename_col)
        if cfg_col_sel and cfg_col_rename:
            c2 = CombinerCSVAdvanced(c, cfg_col_sel=cfg_col_sel, cfg_col_rename=cfg_col_rename)
        elif cfg_col_sel:
            c2 = CombinerCSVAdvanced(c, cfg_col_sel=cfg_col_sel)
        elif cfg_col_rename:
            c2 = CombinerCSVAdvanced(c, cfg_col_rename=cfg_col_rename)
        else:
            c2 = CombinerCSVAdvanced(c)
        df_chk = pd.DataFrame()
        for df in df_chks:
            df_chk = df_chk.append(df)
        table_name = 'test'
        db_cnxn_string = cnxn_string.format('test-combined-adv')
        if stream:
            c2.to_sql_stream(db_cnxn_string, table_name)
        else:
            c2.to_sql(db_cnxn_string, table_name)
        dfc = pd.read_sql("select * from test", db_cnxn_string)
        dfc = dfc.set_index('index')
        dfc.index.name = None
        pd.testing.assert_frame_equal(dfc, df_chk)
        assert dfc.equals(df_chk)

    # rename 1, select all
    l = create_files_csv_rename[:2]
    helper(l, ['a'], {'b': 'a'}, [df11, df11], stream=True)

    # test sql stream
    helper(l, ['a'], {'b': 'a'}, [df11, df11])

    # rename 1, select some
    l = [create_files_csv_rename[2]]
    helper(l, ['a'], {'b': 'a'}, [df11])

    # rename none, select 1
    l = [create_files_csv_rename[2]]
    helper(l, ['a'], None, [df11])

    # rename none, select all
    l = [create_files_csv_rename[2]]
    helper(l, ['a', 'c'], None, [df21])

    # rename none, select all, filename col true
    df21['filename'] = os.path.basename(l[0])
    helper(l, ['a', 'c'], None, [df21], is_filename_col=True)


def test_CombinerCSV_sql(create_files_csv):

    def helper(fnames, is_col_common=False, is_filename_col=False, stream=False):
        combiner = CombinerCSV(fname_list=fnames, all_strings=True, cfg_filename_col=is_filename_col)
        table_name = 'test'
        db_cnxn_string = cnxn_string.format('test-combined-adv')
        if stream:
            combiner.to_sql_stream(db_cnxn_string, table_name, is_col_common=is_col_common)
        else:
            combiner.to_sql(db_cnxn_string, table_name, is_col_common=is_col_common)
        df = pd.read_sql("select * from test", db_cnxn_string)
        df = df.set_index('index')
        df.index.name = None
        return df

    # all columns present, to_sql
    fname_list = create_files_csv
    df_chk = create_files_df_clean_combine()
    assert df_chk.equals(helper(fname_list))

    # to sql stream
    assert df_chk.equals(helper(fname_list, stream=True))

    # columns mismatch, common columns, to_sql
    fname_list = create_files_csv_colmismatch()
    df_chk = create_files_df_colmismatch_combine(cfg_col_common=True)
    assert helper(fname_list, is_col_common=True).shape[1] == df_chk.shape[1]

