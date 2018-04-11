import numpy as np
import pandas as pd
from .helpers_ui import *
from openpyxl.utils import coordinate_from_string


class ExcelAdvanced:
    """

        Excel Advanced Class - Checks columns, generates preview, convert excel to dataframes.

        Raises:
            ValueError: if header_xls_range is invalid or header_xls_start and header_xls_end is invalid

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
    if not (header_xls_start and header_xls_end):
        if header_range:
            header_range = header_range.split(':')
            header_xls_start , header_xls_end = header_range
        else:
            # Return with error message - Discuss with Norman
            raise Exception

    # header, skiprows, usecols
    scol, srow = coordinate_from_string(header_xls_start)
    ecol, erow = coordinate_from_string(header_xls_end)

    header = [x for x in range(erow - srow + 1)]
    usecols = scol + ":" + ecol
    skiprows = srow - 1

    df = read_excel(io, index_col=None, header=header, skiprows=skiprows, usecols=usecols, **kwds)

    if remove_blank_cols:
        df = df.dropna(axis='columns', how='all')
    if remove_blank_rows:
        df = df.dropna(axis='rows', how='all')
    if collapse_header:
        if len(header) > 1:
            df.columns = [' '.join([s for s in col if not 'Unnamed' in s]).strip().replace("\n", ' ')
                          for col in df.columns.values]
        else:
            df = pd.read_excel(io, header=header, skiprows=skiprows, usecols=usecols, **kwds)

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