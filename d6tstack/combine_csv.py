import os
import ntpath

import numpy as np
import pandas as pd

from .sniffer import CSVSnifferList
from .helpers import *
from .helpers_ui import *


# ******************************************************************
# helpers
# ******************************************************************

def sniff_settings_csv(fname_list):
    sniff = CSVSnifferList(fname_list)
    csv_sniff = {}
    csv_sniff['delim'] = sniff.get_delim()
    csv_sniff['skiprows'] = sniff.count_skiprows()
    csv_sniff['has_header'] = sniff.has_header()
    csv_sniff['header'] = 0 if sniff.has_header() else None
    return csv_sniff


def apply_select_rename(dfg, cfg_col_sel, cfg_col_rename):

    if cfg_col_rename:
        # check no naming conflicts
        cfg_col_sel2 = list(set([cfg_col_rename[k] if k in cfg_col_rename.keys() else k for k in dfg.columns.tolist()])) # set of columns after rename
        df_rename_count = collections.Counter(cfg_col_sel2)
        if df_rename_count:
            if max(df_rename_count.values()) > 1: # would the rename create naming conflict?
                raise ValueError('Renaming conflict',[(k,v) for k,v in df_rename_count.items() if v>1])
        dfg = dfg.rename(columns=cfg_col_rename)
    if cfg_col_sel:
        if cfg_col_rename and cfg_col_sel:
            cfg_col_sel = list(set(cfg_col_rename.values()) | set(cfg_col_sel))
        max(collections.Counter(cfg_col_sel).values())
        max(collections.Counter(dfg.columns).values())
        dfg = dfg.reindex(columns=cfg_col_sel)

    return dfg


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
        nrows_preview (boolean): number of rows in preview 
        logger (object): logger object with send_log()

    """

    def __init__(self, fname_list, sep=',', has_header = True, all_strings=False, nrows_preview=3, read_csv_params=None, logger=None):
        self.fname_list = fname_list
        self.all_strings = all_strings
        self.nrows_preview = nrows_preview
        self.read_csv_params = read_csv_params
        if not self.read_csv_params:
            self.read_csv_params = {}
        self.read_csv_params['header'] = 0 if has_header else None
        self.read_csv_params['sep'] = sep
        self.logger = logger
        self.col_preview = None

    def read_csv(self, fname, is_preview=False, chunksize=None):
        cfg_dype = str if self.all_strings else None
        cfg_nrows = self.nrows_preview if is_preview else None
        return pd.read_csv(fname, dtype=cfg_dype, nrows=cfg_nrows, chunksize=chunksize,
                           **self.read_csv_params)

    def read_csv_all(self, msg=None, is_preview=False, chunksize=None, cfg_col_sel=None, cfg_col_rename=None):
        dfl_all = []
        if not cfg_col_sel:
            cfg_col_sel = []
        if not cfg_col_rename:
            cfg_col_rename = {}
        for fname in self.fname_list:
            if self.logger and msg:
                self.logger.send_log(msg + ' ' + ntpath.basename(fname), 'ok')
            df = self.read_csv(fname, is_preview=is_preview, chunksize=chunksize)
            if cfg_col_sel or cfg_col_rename:
                df = apply_select_rename(df, cfg_col_sel, cfg_col_rename)
            df['filename'] = ntpath.basename(fname)
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

        dfl_all = self.read_csv_all(msg='scanning colums of', is_preview=True)

        dfl_all_col = [df.columns.tolist() for df in dfl_all]
        [df.remove('filename') for df in dfl_all_col]
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

    def _preview_available(self):
        if not self.col_preview:
            self.preview_columns()

    def is_all_equal(self):
        """
        Return all files equal after checking if preview_columns has been run. If not run it.

        Returns:
             is_all_equal (boolean): If all files equal?
        """
        self._preview_available()
        return self.col_preview['is_all_equal']

    def is_col_present(self):
        """
        Checks if columns are present

        Returns:
             bool: if columns present
        """
        self._preview_available()
        return self.col_preview['df_columns_present'].reset_index(drop=True)

    def is_col_present_unique(self):
        """
        Shows unique columns by file

        Returns:
             bool: if columns present
        """
        self._preview_available()
        return self.is_col_present().set_index('filename')[self.col_preview['columns_unique']]

    def is_col_present_common(self):
        """
        Shows common columns by file        

        Returns:
             bool: if columns present
        """
        self._preview_available()
        return self.is_col_present().set_index('filename')[self.col_preview['columns_common']]

    def preview_combine(self, is_col_common=False):
        """
        
        Preview of combines all files

        Note:
            Unlike `CombinerCSVAdvanced.combine()` this function supports simple combine operations

        Args:
            is_col_common (bool): keep only common columns? If `false` returns all columns filled with nans

        Returns:
            df_all (dataframe): pandas dataframe with combined data from all files, only self.nrows_preview top rows

        """
        return self.combine(is_col_common, is_preview=True)

    def combine(self, is_col_common=False, is_preview=False):
        """
        
        Combines all files

        Note:
            Unlike `CombinerCSVAdvanced.combine()` this function supports simple combine operations

        Args:
            is_col_common (bool): keep only common columns? If `false` returns all columns filled with nans
            is_preview (bool): read only self.nrows_preview top rows

        Returns:
            df_all (dataframe): pandas dataframe with combined data from all files

        """

        dfl_all = self.read_csv_all('reading full file', is_preview=is_preview)

        if self.logger:
            self.logger.send_log('combining files', 'ok')

        if is_col_common:
            df_all = pd.concat(dfl_all, join='inner')
        else:
            df_all = pd.concat(dfl_all)

        self.df_all = df_all

        return df_all


# ******************************************************************
# advanced
# ******************************************************************

class CombinerCSVAdvanced(object):

    def __init__(self, combiner, cfg_col_sel=None, cfg_col_rename=None):
        self.combiner = combiner
        self.cfg_col_sel = cfg_col_sel
        self.cfg_col_rename = cfg_col_rename

        if max(collections.Counter(cfg_col_sel).values())>1:
            return ValueError('Duplicate entries in cfg_col_sel')
        elif not self.cfg_col_sel:
            self.cfg_col_sel = []

        if not self.cfg_col_rename:
            self.cfg_col_rename = {}

    def preview_combine(self):
        df_all = self.combiner.read_csv_all(msg='reading preview file', is_preview=True, cfg_col_sel=self.cfg_col_sel,
                                            cfg_col_rename=self.cfg_col_rename)
        df_all = pd.concat(df_all)
        return df_all

    def combine_preview_save(self, fname_out):
        df_all_preview = self.preview_combine()
        df_all_preview.to_csv(fname_out, index=False)
        return True

    def combine(self):
        df_all = self.combiner.read_csv_all(msg='reading full file', cfg_col_sel=self.cfg_col_sel,
                                            cfg_col_rename=self.cfg_col_rename)
        df_all = pd.concat(df_all)
        return df_all

    def combine_save(self, fname_out):

        if not self.cfg_col_sel:
            raise ValueError('Need to provide cfg_col_sel in constructor to use combine_save()')

        if not os.path.exists(os.path.dirname(fname_out)):
            os.makedirs(os.path.dirname(fname_out))

        fhandle = open(fname_out, 'w')

        # write header
        cfg_col_sel2 = list(set([self.cfg_col_rename[k] if k in self.cfg_col_rename.keys() else k for k in self.cfg_col_sel])) # set of columns after rename

        df_all_header = pd.DataFrame(columns=cfg_col_sel2 + ['filename', ])
        df_all_header.to_csv(fhandle, header=True, index=False)
        # todo: what if file hasn't header

        for fname in self.combiner.fname_list:
            if self.combiner.logger:
                self.combiner.logger.send_log('processing ' + ntpath.basename(fname), 'ok')
            for df_chunk in self.combiner.read_csv(fname, chunksize=1e5):
                if self.cfg_col_sel or self.cfg_col_rename:
                    df_chunk = apply_select_rename(df_chunk, cfg_col_sel2, self.cfg_col_rename)
                df_chunk['filename'] = ntpath.basename(fname)
                df_chunk.to_csv(fhandle, header=False, index=False)

        return True
