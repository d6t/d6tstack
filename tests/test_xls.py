import pytest
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import xlrd

from d6tstack.convert_xls import *

from tests.test_combine import cfg_fname_base_out_dir
from tests.test_combine import create_files_xls_single, create_files_xlsx_single, create_files_xls_multiple, create_files_xlsx_multiple
from tests.test_combine import write_file_xls

cfg_fname_test_base = 'test.xlsx'


# ************************************************************
# XLSSniffer
# ************************************************************
def test_xls_scan_sheets_single(create_files_xls_single, create_files_xlsx_single):
    def helper(fnames):
        xlsSniffer = XLSSniffer(fnames)
        sheets = xlsSniffer.dict_xls_sheets
        assert np.all([file['sheets_names'] == ['Sheet1'] for file in sheets.values()])
        assert np.all([file['sheets_count'] == 1 for file in sheets.values()])
        assert xlsSniffer.all_same_count()
        assert xlsSniffer.all_same_names()
        assert xlsSniffer.all_contain_sheetname('Sheet1')
        assert xlsSniffer.all_have_idx(0)
        assert not xlsSniffer.all_have_idx(1)

    helper(create_files_xls_single)
    helper(create_files_xlsx_single)


def test_xls_scan_sheets_multipe(create_files_xls_multiple, create_files_xlsx_multiple):
    def helper(fnames):
        xlsSniffer = XLSSniffer(fnames)
        sheets = xlsSniffer.dict_xls_sheets
        assert np.all([file['sheets_names'] == ['Sheet1', 'Sheet2'] for file in sheets.values()])
        assert np.all([file['sheets_count'] == 2 for file in sheets.values()])

    helper(create_files_xls_multiple)
    helper(create_files_xlsx_multiple)


#************************************************************
# read_excel_advanced
#************************************************************
cfg_fname_dir_xls = 'test-data/excel_adv_data/'

def test_read_excel_adv():
    # actual file
    fname = cfg_fname_dir_xls + 'read_excel_adv - sample1.xlsx'
    df = read_excel_advanced(fname, header_xls_start="A8", header_xls_end="O9")
    assert 'Balance' in df.columns
    assert 'Billing Type' in df.columns

    df = read_excel_advanced(fname, header_xls_start="A8", header_xls_end="O9")
    assert 'Balance' in df.columns
    assert 'Billing Type' in df.columns

    fname = cfg_fname_dir_xls + 'read_excel_adv - sample3.xlsx'
    df = read_excel_advanced(fname, header_xls_start="A10", header_xls_end="G10")
    assert 'Product Code' in df.columns

    # synthetic data
    dfc = pd.DataFrame({'a':range(10),'b':range(10)})
    fname = cfg_fname_dir_xls + cfg_fname_test_base
    dfc.to_excel(fname,startrow=1,startcol=1,index=False)

    # basic
    dfr = read_excel_advanced(fname, header_xls_start="B2", header_xls_end="C2")
    assert dfr.equals(dfc)
    dfr = read_excel_advanced(fname, header_xls_range="B2:C2")
    assert dfr.equals(dfc)

    # empty rows/columns
    def helper(dfc,dfc2):
        dfc2.to_excel(fname,startrow=1,startcol=1,index=False)
        dfr = read_excel_advanced(fname, header_xls_range="B2:D2")
        assert dfr.astype(int).reset_index(drop=True).equals(dfc)
        dfr = read_excel_advanced(fname, header_xls_range="B2:D2", remove_blank_cols=False, remove_blank_rows=False)
        assert dfr.equals(dfc2)

    helper(dfc, dfc.reindex(['a', 'c', 'b'], axis=1))
    helper(dfc, dfc.reindex(range(-1,10)).reset_index(drop=True))

    # collapse header
    # todo: complete


#************************************************************
# XLStoBase
#************************************************************

def test_XLStoBase():
    cfg_output_dir = 'testout'
    cfg_fname_base_out_dir2 = os.path.join(cfg_fname_base_out_dir,cfg_output_dir)
    cfg_fname_test1 = os.path.join(cfg_fname_base_out_dir,cfg_fname_test_base)
    cfg_fname_test2 = os.path.join(cfg_fname_base_out_dir2,cfg_fname_test_base)

    with pytest.raises(ValueError) as e:
        x = XLStoBase(if_exists='invalid')

    # output_dir
    if os.path.exists(cfg_fname_base_out_dir2):
        shutil.rmtree(cfg_fname_base_out_dir2)
    assert not os.path.exists(cfg_fname_base_out_dir2)

    x = XLStoBase(output_dir=cfg_fname_base_out_dir2)
    assert os.path.exists(cfg_fname_base_out_dir2)

    fname_out, is_skip = x._get_output_filename(cfg_fname_test1)
    assert Path(fname_out).parts[-2] == cfg_output_dir
    assert not is_skip

    # if_exists
    dfc = pd.DataFrame({'a':range(10),'b':range(10)})
    dfc.to_excel(cfg_fname_test2,index=False)
    fname_out, is_skip = x._get_output_filename(cfg_fname_test1)
    assert is_skip

    x = XLStoBase(output_dir=cfg_fname_base_out_dir2,if_exists='replace')
    fname_out, is_skip = x._get_output_filename(cfg_fname_test1)
    assert not is_skip

    # convert_single
    def helper(sheet_name):
        fname_out = x.convert_single(cfg_fname_test2,sheet_name)
        assert sheet_name in fname_out and fname_out[-4:]=='.csv'
        dfr = pd.read_csv(fname_out)
        assert dfr.equals(dfc)

    helper('Sheet1')
    write_file_xls(dfc, cfg_fname_test2)
    helper('Sheet2')

    # convert advanced
    dfc.to_excel(cfg_fname_test2,startrow=1,startcol=1,index=False)
    fname_out = x.convert_single(cfg_fname_test2, 'Sheet1', header_xls_range="B2:C2")
    dfr = pd.read_csv(fname_out)
    assert dfr.equals(dfc)

#************************************************************
# XLStoCSVMultiFile
#************************************************************
def test_XLStoCSVMultiFile(create_files_xls_single,create_files_xlsx_single):

    # global mode
    def helper1(flist,select_mode,select_val):
        x = XLStoCSVMultiFile(flist,output_dir=cfg_fname_base_out_dir,cfg_xls_sheets_sel_mode=select_mode,cfg_xls_sheets_sel=select_val,if_exists='replace')
        fnames_out = x.convert_all()
        fnames_out_chk = [x._get_output_filename(fname+'-'+str(select_val)+'.csv')[0] for fname in flist]
        assert fnames_out==fnames_out_chk
        assert all([os.path.exists(fname) for fname in fnames_out_chk])

    helper1(create_files_xlsx_single,'name_global','Sheet1')
    helper1(create_files_xls_single,'name_global','Sheet1')
    helper1(create_files_xlsx_single,'idx_global',0)

    # by file mode
    def helper2(flist,select_mode,select_val_list):
        x = XLStoCSVMultiFile(flist,output_dir=cfg_fname_base_out_dir,cfg_xls_sheets_sel_mode=select_mode,cfg_xls_sheets_sel=select_val_list,if_exists='replace')
        fnames_out = x.convert_all()
        fnames_out_chk = [x._get_output_filename(fname+'-'+str(select_val_list[fname])+'.csv')[0] for fname in flist]
        assert fnames_out==fnames_out_chk
        assert all([os.path.exists(fname) for fname in fnames_out_chk])

    helper2(create_files_xlsx_single,'idx',dict(zip(create_files_xlsx_single,[0]*len(create_files_xlsx_single))))
    helper2(create_files_xlsx_single,'name',dict(zip(create_files_xlsx_single,['Sheet1']*len(create_files_xlsx_single))))

    # global advanced
    dfc = pd.DataFrame({'a':range(10),'b':range(10)})
    fname = cfg_fname_dir_xls + cfg_fname_test_base
    dfc.to_excel(fname,startrow=1,startcol=1,index=False)
    x = XLStoCSVMultiFile([fname],output_dir=cfg_fname_base_out_dir,cfg_xls_sheets_sel_mode='name_global',cfg_xls_sheets_sel='Sheet1',if_exists='replace')
    fnames_out = x.convert_all(header_xls_range="B2:C2")
    dfr = pd.read_csv(fnames_out[0])
    assert dfr.equals(dfc)


#************************************************************
# XLStoCSVMultiSheet
#************************************************************
def test_XLStoCSVMultiSheet(create_files_xlsx_multiple):
    x = XLStoCSVMultiSheet(create_files_xlsx_multiple[0],output_dir=cfg_fname_base_out_dir,if_exists='replace')

    fname_out = x.convert_single('Sheet1')
    assert 'Sheet1' in fname_out
    fname_out = x.convert_single('Sheet2')
    assert 'Sheet2' in fname_out

    with pytest.raises(xlrd.XLRDError) as e:
        x.convert_single('Sheet3')

    dfc = pd.DataFrame({'a':range(10),'b':range(10)})
    fname = cfg_fname_dir_xls + cfg_fname_test_base
    write_file_xls(dfc, fname, startrow=1, startcol=1)

    x = XLStoCSVMultiSheet(fname,output_dir=cfg_fname_base_out_dir,if_exists='replace')
    fname_out = x.convert_single('Sheet1',header_xls_range="B2:C2")
    assert 'Sheet1' in fname_out
    dfr = pd.read_csv(fname_out)
    assert dfr.equals(dfc)

    fnames_out = x.convert_all(header_xls_range="B2:C2")
    assert len(fnames_out)==2
    assert 'Sheet1' in fnames_out[0]
    assert 'Sheet2' in fnames_out[1]
    dfr = pd.read_csv(fnames_out[0])
    assert dfr.equals(dfc)
    dfr = pd.read_csv(fnames_out[1])
    assert dfr.equals(dfc)

    x = XLStoCSVMultiSheet(fname,sheet_names=['Sheet1'],output_dir=cfg_fname_base_out_dir,if_exists='replace')
    fnames_out = x.convert_all(header_xls_range="B2:C2")
    assert len(fnames_out)==1
    assert 'Sheet1' in fnames_out[0]
