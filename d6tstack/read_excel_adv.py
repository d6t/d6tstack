import numpy as np
import pandas as pd
from openpyxl.utils import coordinate_from_string


def read_excel_advanced(fname, remove_blank_cols=True, remove_blank_rows=True, collapse_header=True,
                        header_xls_range=None, header_xls_start=None, header_xls_end=None, 
                        is_preview=False, nrows_preview=3, logger=None, **kwds):
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
        logger (object): logger object with `logger.send_log('msg','status')`
        sheetname (mixed): see `pandas.read_excel()` documentation

    Returns:
         df (dataframe): pandas dataframe

    Note:
        You can pass in any `pandas.read_excel()` parameters in particular `sheetname`

    """
    header = []
    if header_xls_start and header_xls_end:
        scol, srow = coordinate_from_string(header_xls_start)
        ecol, erow = coordinate_from_string(header_xls_end)

        # header, skiprows, usecols
        header = [x for x in range(erow - srow + 1)]
        usecols = scol + ":" + ecol
        skiprows = srow - 1

        if is_preview:
            workbook = pd.ExcelFile(io)

            rows = workbook.book.sheet_by_index(0).nrows

            # Get only preview rows. Way to implement nrows (in read_csv)
            skip_footer = (rows - skiprows - nrows_preview)

            df = pd.read_excel(io, header=header, skiprows=skiprows, usecols=usecols,
                               skip_footer=skip_footer, **kwds)
        else:
            df = pd.read_excel(io, header=header, skiprows=skiprows, usecols=usecols, **kwds)
            # TODO: catch when user provides `skiprows` or `usecols`
    else:
        df = pd.read_excel(io, **kwds)

    if remove_blank_cols:
        df = df.dropna(axis='columns', how='all')
    if remove_blank_rows:
        df = df.dropna(axis='rows', how='all')
    if collapse_header:
        if len(header) > 1:
            df.columns = [' '.join([s for s in col if not 'Unnamed' in s]).strip().replace("\n", ' ')
                          for col in df.columns.values]
        else:
            df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)
    return df
