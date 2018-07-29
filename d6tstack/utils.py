import numpy as np
import pandas as pd
from openpyxl.utils import coordinate_from_string
from d6tstack.helpers import compare_pandas_versions


def read_excel_advanced(fname, remove_blank_cols=True, remove_blank_rows=True, collapse_header=True,
                        header_xls_range=None, header_xls_start=None, header_xls_end=None,
                        is_preview=False, nrows_preview=3, **kwds):
    """
    Read Excel files to pandas dataframe with advanced options like set header ranges and remove blank columns and rows

    Args:
        fname (str): Excel file path
        remove_blank_cols (bool): remove blank columns
        remove_blank_rows (bool): remove blank rows
        collapse_header (bool): to convert multiline header to a single line string
        header_xls_range (string): range of headers in excel, eg: A4:B16
        header_xls_start (string): Starting cell of excel for header range, eg: A4
        header_xls_end (string): End cell of excel for header range, eg: B16
        is_preview (bool): Read only first `nrows_preview` lines
        nrows_preview (integer): Initial number of rows to be used for preview columns (default: 3)
        kwds (mixed): parameters for `pandas.read_excel()` to pass through

    Returns:
         df (dataframe): pandas dataframe

    Note:
        You can pass in any `pandas.read_excel()` parameters in particular `sheet_name`

    """
    header = []

    if header_xls_range:
        if not (header_xls_start and header_xls_end):
            header_xls_range = header_xls_range.split(':')
            header_xls_start, header_xls_end = header_xls_range
        else:
            raise ValueError('Parameter conflict. Can only pass header_xls_range or header_xls_start with header_xls_end')

    if header_xls_start and header_xls_end:
        if 'skiprows' in kwds or 'usecols' in kwds:
            raise ValueError('Parameter conflict. Cannot pass skiprows or usecols with header_xls')

        scol, srow = coordinate_from_string(header_xls_start)
        ecol, erow = coordinate_from_string(header_xls_end)

        # header, skiprows, usecols
        header = list(range(erow - srow + 1))
        usecols = scol + ":" + ecol
        skiprows = srow - 1

        if compare_pandas_versions(pd.__version__, "0.20.3") > 0:
            df = pd.read_excel(fname, header=header, skiprows=skiprows, usecols=usecols, **kwds)
        else:
            df = pd.read_excel(fname, header=header, skiprows=skiprows, parse_cols=usecols, **kwds)
    else:
        df = pd.read_excel(fname, **kwds)

    # remove blank cols and rows
    if remove_blank_cols:
        df = df.dropna(axis='columns', how='all')
    if remove_blank_rows:
        df = df.dropna(axis='rows', how='all')

    # todo: add df.reset_index() once no actual data in index

    # clean up header
    if collapse_header:
        if len(header) > 1:
            df.columns = [' '.join([s for s in col if not 'Unnamed' in s]).strip().replace("\n", ' ')
                          for col in df.columns.values]
            df = df.reset_index()
        else:
            df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)

    # preview
    if is_preview:
        df = df.head(nrows_preview)

    return df


class PrintLogger(object):
    def send_log(self, msg, status):
        print(msg,status)

    def send(self, data):
        print(data)

