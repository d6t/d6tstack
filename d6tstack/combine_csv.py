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
        if cfg_col_rename:
            cfg_col_sel2 = list(set([cfg_col_rename[k] if k in cfg_col_rename.keys() else k for k in cfg_col_sel])) # set of columns after rename
        else:
            cfg_col_sel2 = cfg_col_sel
        dfg = dfg.reindex(columns=cfg_col_sel2)

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

    def __init__(self, fname_list, sep=',', has_header = True, all_strings=False, nrows_preview=3, read_csv_params=None,
                 logger=None):
        if not fname_list:
            raise ValueError("Filename list should not be empty")
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

    def read_csv_all(self, msg=None, is_preview=False, chunksize=None, cfg_col_sel=None, cfg_col_rename=None,
                     is_filename_col=True):
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
            if is_filename_col:
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

    def combine(self, is_col_common=False, is_preview=False, is_filename_col=True):
        """
        
        Combines all files

        Note:
            Unlike `CombinerCSVAdvanced.combine()` this function supports simple combine operations

        Args:
            is_col_common (bool): keep only common columns? If `false` returns all columns filled with nans
            is_preview (bool): read only self.nrows_preview top rows
            is_filename_col (bool): add filename column to output data frame. If `False`, will not add column.

        Returns:
            df_all (dataframe): pandas dataframe with combined data from all files

        """

        dfl_all = self.read_csv_all('reading full file', is_preview=is_preview, is_filename_col=is_filename_col)

        if self.logger:
            self.logger.send_log('combining files', 'ok')

        if is_col_common:
            df_all = pd.concat(dfl_all, join='inner')
        else:
            df_all = pd.concat(dfl_all)

        self.df_all = df_all

        return df_all

    def get_output_filename(self, fname, suffix):
        basename = os.path.basename(fname)
        name_with_ext = os.path.splitext(basename)
        new_name = name_with_ext[0] + suffix
        if len(name_with_ext) == 2:
            new_name += name_with_ext[1]
        return new_name

    def create_output_dir(self, output_dir):
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def get_columns_for_save(self, is_filename_col=True, is_col_common=False):
        self._preview_available()
        columns = self.col_preview['columns_common'] if is_col_common else self.col_preview['columns_all']
        if is_filename_col:
            columns += ['filename', ]
        return columns

    def save_files(self, columns, out_filename=None, output_dir=None, suffix='-matched', overwrite=True, chunksize=1e10,
                   is_filename_col=True, cfg_col_sel2=None, cfg_col_rename=None):

        df_all_header = pd.DataFrame(columns=columns)
        if out_filename:
            fhandle = open(out_filename, 'w')
            df_all_header.to_csv(fhandle, header=True, index=False)
        for fname in self.fname_list:
            if self.logger:
                self.logger.send_log('processing ' + ntpath.basename(fname), 'ok')

            new_name = self.get_output_filename(fname, suffix)
            if output_dir:
                fname_out = os.path.join(output_dir, new_name)
            else:
                fname_out = os.path.join(os.path.dirname(fname), new_name)
            if overwrite or not os.path.isfile(fname_out):
                # todo: warning to be raised - how?
                if not out_filename:
                    fhandle = open(fname_out, 'w')
                    df_all_header.to_csv(fhandle, header=True, index=False)
                for df_chunk in self.read_csv(fname, chunksize=chunksize):
                    if cfg_col_sel2 or cfg_col_rename:
                        df_chunk = apply_select_rename(df_chunk, cfg_col_sel2, cfg_col_rename)
                    if is_filename_col:
                        df_chunk['filename'] = ntpath.basename(new_name)
                    df_chunk.to_csv(fhandle, header=False, index=False)

        return True

    def align_save(self, output_dir=None, suffix='-matched', overwrite=True, chunksize=1e10, is_filename_col=True,
                   is_col_common=False):
        """

        Save matched columns data directly to CSV for each of the files.

        Args:
            output_dir (str): output directory to save, default input file directory, optional
            suffix (str): suffix to add to end of screen to input filename to create output file name, optional
            overwrite (bool): overwrite file if exists, default True, optional
            is_col_common (bool): Use common columns else all columns, default False, optional

        """
        cfg_col_sel2 = self.get_columns_for_save()
        columns = cfg_col_sel2
        if is_filename_col:
            columns += ['filename', ]

        return self.save_files(columns, output_dir=output_dir, suffix=suffix, overwrite=overwrite,
                               chunksize=chunksize, is_filename_col=is_filename_col,
                               cfg_col_sel2=cfg_col_sel2)



# ******************************************************************
# advanced
# ******************************************************************


class CombinerCSVAdvanced(object):
    """

    Combiner class with advanced features. Allows renaming, selecting of columns and out-of-core combining

    Args:
        combiner (object): instance of CombinerCSV
        cfg_col_sel (list): list of column names to keep
        cfg_col_rename (dict): dict of columns to rename `{'name_old':'name_new'}

    """

    def __init__(self, combiner: CombinerCSV, cfg_col_sel=None, cfg_col_rename=None):
        self.combiner = combiner
        self.cfg_col_sel = cfg_col_sel
        self.cfg_col_rename = cfg_col_rename

        if not self.cfg_col_sel:
            self.cfg_col_sel = []
        else:
            if max(collections.Counter(cfg_col_sel).values())>1:
                raise ValueError('Duplicate entries in cfg_col_sel')

        if not self.cfg_col_rename:
            self.cfg_col_rename = {}

    def preview_combine(self):
        """

        Preview of combines all files

        Returns:
            df_all (dataframe): pandas dataframe with combined data from all files, only self.combiner.nrows_preview top rows

        """
        df_all = self.combiner.read_csv_all(msg='reading preview file', is_preview=True, cfg_col_sel=self.cfg_col_sel,
                                            cfg_col_rename=self.cfg_col_rename)
        df_all = pd.concat(df_all)
        return df_all

    def combine_preview_save(self, fname_out):
        """

        Save preview to CSV

        Args:
            fname_out (str): filename

        """
        df_all_preview = self.preview_combine()
        df_all_preview.to_csv(fname_out, index=False)
        return True

    def combine(self, is_filename_col=True):
        """

        Combines all files. This is in-memory. For out-of-core use `combine_save()`

        Returns:
            df_all (dataframe): pandas dataframe with combined data from all files

        """
        df_all = self.combiner.read_csv_all(msg='reading full file', cfg_col_sel=self.cfg_col_sel,
                                            cfg_col_rename=self.cfg_col_rename, is_filename_col=is_filename_col)
        df_all = pd.concat(df_all)
        return df_all

    def get_columns_for_save(self):
        if not self.cfg_col_sel:
            raise ValueError('Need to provide cfg_col_sel in constructor to use align_save()')

        # set of columns after rename
        cfg_col_sel2 = list(collections.OrderedDict.fromkeys([self.cfg_col_rename[k]
                                                              if k in self.cfg_col_rename.keys() else k
                                                              for k in self.cfg_col_sel]))

        return cfg_col_sel2

    def combine_save(self, fname_out, chunksize=1e10, is_filename_col=True):
        """

        Save combined data directly to CSV. This implements out-of-core combine functionality to combine large files. For in-memory use `combine()`

        Args:
            fname_out (str): filename

        """
        cfg_col_sel2 = self.get_columns_for_save()

        columns = cfg_col_sel2
        if is_filename_col:
            columns += ['filename', ]

        self.combiner.create_output_dir(os.path.dirname(fname_out))

        return self.combiner.save_files(columns, chunksize=chunksize, is_filename_col=is_filename_col,
                                        cfg_col_sel2=cfg_col_sel2, cfg_col_rename=self.cfg_col_rename,
                                        overwrite=True)

    def align_save(self, output_dir=None, suffix='-matched', overwrite=True, chunksize=1e10, is_filename_col=True):
        """

        Save files aligning the columns for large files. For combined save use `combine_save()`

        Args:
            output_dir (str): output directory to save, default input file directory, optional
            suffix (str): suffix to add to end of screen to input filename to create output file name, optional
            overwrite (bool): overwrite file if exists, default True, optional

        """
        cfg_col_sel2 = self.get_columns_for_save()
        columns = cfg_col_sel2
        if is_filename_col:
            columns += ['filename', ]

        self.combiner.save_files(columns, output_dir=output_dir, suffix=suffix, overwrite=overwrite,
                                 chunksize=chunksize, is_filename_col=is_filename_col, cfg_col_sel2=cfg_col_sel2,
                                 cfg_col_rename=self.cfg_col_rename)

        return True
