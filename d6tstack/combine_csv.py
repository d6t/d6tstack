import numpy as np
import pandas as pd
pd.set_option('display.expand_frame_repr', False)
from scipy.stats import mode
import warnings
import ntpath

from .helpers import *


# ******************************************************************
# combiner
# ******************************************************************

class CombinerCSV(object):
    """    
    Core combiner class. Checks columns, generates preview, combines.

    Args:
        fname_list (list): file names, eg ['a.csv','b.csv']
        sep (string): CSV delimiter, see pandas.read_csv()
        has_header (boolean): data has header row 
        all_strings (boolean): read all values as strings (faster) 
        header_row (int): header row, see pandas.read_csv()
        skiprows (int): rows to skip at top of file, see pandas.read_csv()
        nrows_preview (int): number of rows in preview
        add_filename (bool): add filename column to output data frame. If `False`, will not add column.
        columns_select (list): list of column names to keep
        columns_rename (dict): dict of columns to rename `{'name_old':'name_new'}
        apply_after_read (function): function to apply after reading each file. needs to return a dataframe
        logger (object): logger object with send_log()

    """

    def __init__(self, fname_list, sep=',', has_header = True, all_strings=False, nrows_preview=3, read_csv_params=None,
                 add_filename=True, columns_select=None, columns_rename=None, apply_after_read=None, chunksize=3, logger=None):
        if not fname_list:
            raise ValueError("Filename list should not be empty")
        self.fname_list = np.sort(fname_list)
        self.all_strings = all_strings
        self.nrows_preview = nrows_preview
        self.read_csv_params = read_csv_params
        if not self.read_csv_params:
            self.read_csv_params = {}
        self.read_csv_params['header'] = 0 if has_header else None
        self.read_csv_params['sep'] = sep
        self.logger = logger
        self.sniff_results = None
        self.add_filename = add_filename
        self.columns_select = columns_select
        self.columns_rename = columns_rename
        self.apply_after_read = apply_after_read
        self.chunksize = chunksize

        if not self.columns_select:
            self.columns_select = []
        else:
            if max(collections.Counter(columns_select).values())>1:
                raise ValueError('Duplicate entries in columns_select')

        if not self.columns_rename:
            self.columns_rename = {}

    def read_csv_all(self, msg=None, is_preview=False):

        return dfl_all

    def sniff_columns(self):

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

        if self.logger:
            self.logger.send_log('sniffing columns', 'ok')

        # read nrows of every file
        dfl_all = []
        for fname in self.fname_list:
            # todo: make sure no nrows param in self.read_csv_params
            df = pd.read_csv(fname, dtype=str, nrows=self.nrows_preview,**self.read_csv_params)
            dfl_all.append(df)

        # process columns
        dfl_all_col = [df.columns.tolist() for df in dfl_all]
        col_files = dict(zip(self.fname_list, dfl_all_col))
        col_common = list_common(list(col_files.values()))
        col_all = list_unique(list(col_files.values()))

        # find index in column list so can check order is correct
        df_col_present = {}
        for iFileName, iFileCol in col_files.items():
            df_col_present[iFileName] = [iCol in iFileCol for iCol in col_all]

        df_col_present = pd.DataFrame(df_col_present, index=col_all).T
        df_col_present.index.names = ['file_path']

        # find index in column list so can check order is correct
        df_col_idx = {}
        for iFileName, iFileCol in col_files.items():
            df_col_idx[iFileName] = [iFileCol.index(iCol) if iCol in iFileCol else np.nan for iCol in col_all]
        df_col_idx = pd.DataFrame(df_col_idx, index=col_all).T

        # order columns by where they appear in file
        m=mode(df_col_idx,axis=0)
        df_col_pos = pd.DataFrame({'o':m[0][0],'c':m[1][0]},index=df_col_idx.columns)
        df_col_pos = df_col_pos.sort_values(['o','c'])
        df_col_pos['iscommon']=df_col_pos.index.isin(col_common)


        # reorder by position
        col_all = df_col_pos.index.values.tolist()
        col_common = df_col_pos[df_col_pos['iscommon']].index.values.tolist()
        col_unique = df_col_pos[~df_col_pos['iscommon']].index.values.tolist()
        df_col_present = df_col_present[col_all]
        df_col_idx = df_col_idx[col_all]

        sniff_results = {'files_columns': col_files, 'columns_all': col_all, 'columns_common': col_common,
                       'columns_unique': col_unique, 'is_all_equal': columns_all_equal(dfl_all_col),
                       'df_columns_present': df_col_present, 'df_columns_order': df_col_idx}
        self.sniff_results = sniff_results

        return sniff_results

    def get_sniff_results(self):
        if not self.sniff_results:
            self.sniff_columns()
        return self.sniff_results

    def _preview_available(self):
        if not self.sniff_results:
            self.sniff_columns()

    def is_all_equal(self):
        """
        Return all files equal after checking if preview_columns has been run. If not run it.

        Returns:
             is_all_equal (boolean): If all files equal?
        """
        self._preview_available()
        return self.sniff_results['is_all_equal']

    def is_column_present(self):
        """
        Checks if columns are present

        Returns:
             bool: if columns present
        """
        self._preview_available()
        return self.sniff_results['df_columns_present']

    def is_column_present_unique(self):
        """
        Shows unique columns by file

        Returns:
             bool: if columns present
        """
        self._preview_available()
        return self.is_column_present()[self.sniff_results['columns_unique']]

    def is_column_present_common(self):
        """
        Shows common columns by file        

        Returns:
             bool: if columns present
        """
        self._preview_available()
        return self.is_column_present()[self.sniff_results['columns_common']]

