"""Run unit tests.

Use this to run tests and understand how tasks.py works.

Example:

    Create directories::

        mkdir -p test-data/input
        mkdir -p test-data/output

    Run tests::

        pytest test_excel_adv.py -s

Notes:

    * this will create sample csv, xls and xlsx files
    * test_combine_() test the main combine function

"""

from d6t.stack.stack.read_excel_adv import *

fname_base_input_dir = 'test-data/excel_adv_data/'
fname_base_out_dir = 'test-data/output/'


def test_read_excel_adv():
    fname = fname_base_input_dir + 'read_excel_adv - sample1.xlsx'
    ea = ExcelAdvanced([fname], remove_blank_cols=True,
                       remove_blank_rows=True, collapse_header=True, header_xls_start="A8",
                       header_xls_end="O9")
    df = ea.read_excel_adv(fname)
    assert 'Balance' in df.columns
    assert 'Billing Type' in df.columns

    fname = fname_base_input_dir + 'read_excel_adv - sample3.xlsx'
    ea = ExcelAdvanced([fname], remove_blank_cols=True,
                       remove_blank_rows=True, collapse_header=True, header_xls_start="A10",
                       header_xls_end="G10")
    df = ea.read_excel_adv(fname)
    assert 'Product Code' in df.columns

    fname1 = fname_base_input_dir + 'test-col-sample1.xlsx'
    fname2 = fname_base_input_dir + 'test-col-sample2.xlsx'
    ea = ExcelAdvanced([fname1, fname2], remove_blank_cols=True,
                       remove_blank_rows=True, collapse_header=True, header_xls_start="B23",
                       header_xls_end="I24")
    column_preview = ea.preview_columns()
    assert column_preview['is_all_equal']

    fname3 = fname_base_input_dir + 'test-col-sample3.xlsx'
    ea = ExcelAdvanced([fname1, fname2, fname3], remove_blank_cols=True,
                       remove_blank_rows=True, collapse_header=True, header_xls_start="B23",
                       header_xls_end="I24")
    column_preview = ea.preview_columns()
    assert not column_preview['is_all_equal']