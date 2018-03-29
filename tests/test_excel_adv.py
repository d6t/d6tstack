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

from d6t.stack.read_excel_adv import *

fname_base_input_dir = 'test-data/excel_adv_data/'
fname_base_out_dir = 'test-data/output/'


def test_read_excel_adv():
    fname = fname_base_input_dir + 'test-data-restaurants.xlsx'
    df = read_excel_adv(fname, sheet_name=3, remove_blank_cols=True,
                        remove_blank_rows=True, collapse_header=True, header_xls_start="B23",
                        header_xls_end="I24")
    assert 'End Date' in df.columns
    assert 'Year-over-Year Change Sales Index' in df.columns

    fname = fname_base_input_dir + 'read_excel_adv - sample3.xlsx'
    df = read_excel_adv(fname, remove_blank_cols=True,
                        remove_blank_rows=True, collapse_header=True, header_xls_start="A10",
                        header_xls_end="G10")
    assert 'Product Code' in df.columns
