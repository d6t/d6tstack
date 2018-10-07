import numpy as np
import pandas as pd
pd.set_option('display.expand_frame_repr', False)
from scipy.stats import mode
import warnings
import ntpath
import copy

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
                 columns_select=None, columns_select_common=False, columns_rename=None, add_filename=True, output_dir = None, output_prefix = 'd6stack-',
                 apply_after_read=None, chunksize=3, logger=None):
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
        self.read_csv_params.pop('chunksize', None)
        self.logger = logger
        self.sniff_results = None
        self.add_filename = add_filename
        self.columns_select = columns_select
        self.columns_select_common = columns_select_common
        if columns_select and columns_select_common:
            warnings.warn('columns_select will override columns_select_common, pick either one')
        self.columns_rename = columns_rename
        self._columns_reindex = None
        self._columns_rename_dict = None
        self.apply_after_read = apply_after_read
        self.chunksize = chunksize
        self.output_dir = output_dir
        self.output_prefix = output_prefix

        if not self.columns_select:
            self.columns_select = []
        else:
            if max(collections.Counter(columns_select).values())>1:
                raise ValueError('Duplicate entries in columns_select')

        if not self.columns_rename:
            self.columns_rename = {}

    def _read_csv_yield(self, fname):
        dfs = pd.read_csv(fname, **self.read_csv_params, chunksize=self.chunksize)
        for dfc in dfs:
            if self.columns_rename and self._columns_rename_dict[fname]:
                dfc = dfc.rename(columns=self._columns_rename_dict[fname])

            if dfc.columns.tolist != self._columns_reindex:
                dfc = dfc.reindex(columns=self._columns_reindex)
            if self.apply_after_read:
                dfc = self.apply_after_read(dfc)
            if self.add_filename:
                dfc['filepath'] = fname
                dfc['filename'] = ntpath.basename(fname)
            yield dfc

    def sniff_columns(self):

        """
        
        Checks column consistency by reading top nrows in all files. It checks both presence and order of columns in all files

        Returns:
            dict: results dictionary with
                files_columns (dict): dictionary with information, keys = filename, value = list of columns in file
                columns_all (list): all columns in files
                columns_common (list): only columns present in every file
                is_all_equal (boolean): all files equal in all files?
                df_columns_present (dataframe): which columns are present in which file?
                df_columns_order (dataframe): where in the file is the column?

        """

        if self.logger:
            self.logger.send_log('sniffing columns', 'ok')

        read_csv_params = copy.deepcopy(self.read_csv_params)
        read_csv_params['dtype'] = str
        read_csv_params['nrows'] = self.nrows_preview

        # read nrows of every file
        dfl_all = []
        for fname in self.fname_list:
            # todo: make sure no nrows param in self.read_csv_params
            df = pd.read_csv(fname, **self.read_csv_params)
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

    def _sniff_available(self):
        if not self.sniff_results:
            self.sniff_columns()

    def is_all_equal(self):
        """
        Checks if all columns are equal in all files

        Returns:
             bool: all columns are equal in all files?
        """
        self._sniff_available()
        return self.sniff_results['is_all_equal']

    def is_column_present(self):
        """
        Shows which columns are present in which files

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self._sniff_available()
        return self.sniff_results['df_columns_present']

    def is_column_present_unique(self):
        """
        Shows unique columns by file

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self._sniff_available()
        return self.is_column_present()[self.sniff_results['columns_unique']]

    def is_column_present_common(self):
        """
        Shows common columns by file        

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self._sniff_available()
        return self.is_column_present()[self.sniff_results['columns_common']]

    def _columns_reindex_prep(self):

        self._sniff_available()
        self._columns_select_dict = {} # select columns by filename
        self._columns_rename_dict = {} # rename columns by filename

        for fname in self.fname_list:
            columns_rename = self.columns_rename.copy()
            if self.columns_rename:
                # check no naming conflicts
                columns_select2 = [columns_rename[k] if k in columns_rename.keys() else k for k in self.sniff_results['files_columns'][fname]]
                df_rename_count = collections.Counter(columns_select2)
                if df_rename_count and max(df_rename_count.values()) > 1:  # would the rename create naming conflict?
                    warnings.warn('Renaming conflict: {}'.format([(k, v) for k, v in df_rename_count.items() if v > 1]),
                                  UserWarning)
                    while df_rename_count and max(df_rename_count.values()) > 1:
                        # remove key value pair causing conflict
                        conflicting_keys = [i for i, j in df_rename_count.items() if j > 1]
                        columns_rename = {k: v for k, v in columns_rename.items() if k in conflicting_keys}
                        columns_select2 = [columns_rename[k] if k in columns_rename.keys() else k for k in
                                           self.sniff_results['files_columns'][fname]]
                        df_rename_count = collections.Counter(columns_select2)

                # store rename by file. keep only renames for columns actually present in file
                self._columns_rename_dict[fname] = dict((k,v) for k,v in columns_rename.items() if k in k in self.sniff_results['files_columns'][fname])

        if self.columns_select:
            columns_select2 = self.columns_select.copy()
        else:
            if self.columns_select_common:
                columns_select2 = self.sniff_results['columns_common'].copy()
            else:
                columns_select2 = self.sniff_results['columns_all'].copy()

        if columns_rename:
            columns_select2 = list(dict.fromkeys([columns_rename[k] if k in columns_rename.keys() else k for k in columns_select2]))  # set of columns after rename
        # store select by file
        self._columns_reindex = columns_select2

    def _columns_reindex_available(self):
        if not self._columns_rename_dict or not self._columns_reindex:
            self._columns_reindex_prep()

    def preview_rename(self):
        self._columns_reindex_available()
        df = pd.DataFrame(self._columns_rename_dict).T
        return df

    def preview_select(self):
        self._columns_reindex_available()
        return self._columns_reindex

    def to_pandas(self):
        self._columns_reindex_available()
        df = [[dfc for dfc in self._read_csv_yield(fname)] for fname in self.fname_list]
        import itertools
        df = pd.concat(itertools.chain.from_iterable(df), sort=False, copy=False)
        return df
