import numpy as np
import pandas as pd
from .helpers_ui import *
from openpyxl.utils import coordinate_from_string


def read_excel_advanced(fname, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                        header_xls_range=None, header_xls_start=None, header_xls_end=None, nrows_preview=3,
                        logger=None, **kwds):
    """
    Excel Advanced Function - Set header ranges and remove blank columns and rows while converting excel to dataframe

    Args:
        fname_list (list): excel file names, eg ['a.xls','b.xls']
        remove_blank_cols (boolean): to remove blank columns in output (default: False)
        remove_blank_rows (boolean): to remove blank rows in output (default: False)
        collapse_header (boolean): to convert multiline header to a single line string (default: False)
        header_xls_range (string): range of headers in excel, eg: A4:B16
        header_xls_start (string): Starting cell of excel for header range, eg: A4
        header_xls_end (string): End cell of excel for header range, eg: B16
        nrows_preview (integer): Initial number of rows to be used for preview columns (default: 3)
        log_pusher (object): logger object that sends pusher logs

    Returns:
         df (dataframe): pandas dataframe
    """
    ea = ExcelAdvanced([fname], remove_blank_cols=remove_blank_cols, remove_blank_rows=remove_blank_rows,
                       collapse_header=collapse_header, header_xls_range=header_xls_range,
                       header_xls_start=header_xls_start, header_xls_end=header_xls_end, nrows_preview=nrows_preview,
                       logger=logger)
    return ea.read_excel_adv(fname, **kwds)


class ExcelAdvanced:
    """

        Excel Advanced Class - Checks columns, generates preview, convert excel to dataframes.

        Args:
            fname_list (list): excel file names, eg ['a.xls','b.xls']
            remove_blank_cols (boolean): to remove blank columns in output (default: False)
            remove_blank_rows (boolean): to remove blank rows in output (default: False)
            collapse_header (boolean): to convert multiline header to a single line string (default: False)
            header_xls_range (string): range of headers in excel, eg: A4:B16
            header_xls_start (string): Starting cell of excel for header range, eg: A4
            header_xls_end (string): End cell of excel for header range, eg: B16
            nrows_preview (integer): Initial number of rows to be used for preview columns (default: 3)
            log_pusher (object): logger object that sends pusher logs

    """

    def __init__(self, fname_list, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                 header_xls_range=None, header_xls_start=None, header_xls_end=None, nrows_preview=3, logger=None):
        self.fname_list = fname_list
        self.remove_blank_cols = remove_blank_cols
        self.remove_blank_rows = remove_blank_rows
        self.collapse_header = collapse_header
        if not (header_xls_start and header_xls_end):
            if header_xls_range:
                header_xls_range = header_xls_range.split(':')
                header_xls_start , header_xls_end = header_xls_range
        self.header_xls_start = header_xls_start
        self.header_xls_end = header_xls_end
        self.nrows_preview = nrows_preview
        self.logger = logger

    def read_excel_adv(self, io, is_preview=False, **kwds):
        """
        # TODO: Handle multiple sheets at once. Each sheet may have difference col and rows range.
        Args:
            io (string): excel file name or pandas ExcelFile object
            is_preview (boolean): to get the dataframe with preview rows only.
        Returns:
             dataframe
        """
        header = []
        if self.header_xls_start and self.header_xls_end:
            scol, srow = coordinate_from_string(self.header_xls_start)
            ecol, erow = coordinate_from_string(self.header_xls_end)

            # header, skiprows, usecols
            header = [x for x in range(erow - srow + 1)]
            usecols = scol + ":" + ecol
            skiprows = srow - 1

            if is_preview:
                workbook = pd.ExcelFile(io)

                rows = workbook.book.sheet_by_index(0).nrows

                # Get only preview rows. Way to implement nrows (in read_csv)
                skip_footer = (rows - skiprows - self.nrows_preview)

                df = pd.read_excel(io, header=header, skiprows=skiprows, usecols=usecols,
                                   skip_footer=skip_footer, **kwds)
            else:
                df = pd.read_excel(io, header=header, skiprows=skiprows, usecols=usecols, **kwds)
        else:
            df = pd.read_excel(io, **kwds)

        if self.remove_blank_cols:
            df = df.dropna(axis='columns', how='all')
        if self.remove_blank_rows:
            df = df.dropna(axis='rows', how='all')
        if self.collapse_header:
            if len(header) > 1:
                df.columns = [' '.join([s for s in col if not 'Unnamed' in s]).strip().replace("\n", ' ')
                              for col in df.columns.values]
            else:
                df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)
        return df

    def read_excel_adv_all(self, msg=None, is_preview=False):
        dfl_all = []
        for fname in self.fname_list:
            if self.logger and msg:
                self.logger.send_log(msg + ' ' + ntpath.basename(fname), 'ok')
            df = self.read_excel_adv(fname, is_preview)
            dfl_all.append(df)
        return dfl_all

    def preview_columns(self):
        """

        Checks column consistency in list of files. It checks both presence and order of columns in all files

        Returns:
            col_preview (dict): results dictionary with
                files_columns (dict): dictionary with information, keys = filename, value = list of columns in file
                columns_all (list): all columns in files
                columns_common (list): only columns present in every file
                is_all_equal (boolean): all files equal in all files?
                df_columns_present (dataframe): which columns are present in which file?
                df_columns_order (dataframe): where in the file is the column?

        """
        dfl_all = self.read_excel_adv_all(msg='scanning colums of', is_preview=True)
        dfl_all_col = [df.columns.tolist() for df in dfl_all]
        col_files = dict(zip(self.fname_list, dfl_all_col))
        col_common = list_common(list(col_files.values()))
        col_all = list_unique(list(col_files.values()))
        col_unique = list(set(col_all) - set(col_common))

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

        col_preview = {'files_columns': col_files, 'columns_all': col_all, 'columns_common': col_common,
                       'columns_unique': col_unique, 'is_all_equal': columns_all_equal(dfl_all_col),
                       'df_columns_present': df_col_present, 'df_columns_order': df_col_order}
        self.col_preview = col_preview

        return col_preview