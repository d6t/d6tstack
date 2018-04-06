import os
import ntpath

import numpy as np
import pandas as pd

from .sniffer import CSVSnifferList
from .helpers import *
from .helpers_ui import *

#******************************************************************
# combiner
#******************************************************************

def sniff_settings_csv(fname_list):
    sniff = CSVSnifferList(fname_list)
    csv_sniff = {}
    csv_sniff['delim'] = sniff.get_delim()
    csv_sniff['skiprows'] = sniff.count_skiprows()
    csv_sniff['has_header'] = sniff.has_header()
    csv_sniff['header'] = 0 if sniff.has_header() else None
    return csv_sniff

class CombinerCSV(object):
    """
    
    Core combiner class. Checks columns, generates preview, combines.

    Args:
        fname_list (list): file names, eg ['a.csv','b.csv']
        sep (string): CSV delimiter, see pandas.read_csv()
        all_strings (boolean): read all values as strings (faster) 
        header_row (int): header row, see pandas.read_csv()
        skiprows (int): rows to skip at top of file, see pandas.read_csv()
        nrows_preview (boolean): number of rows in preview 
        logger (object): logger object with send_log()

    """

    def __init__(self, fname_list, sep=',', all_strings = False, header_row = 0, skiprows=0, nrows_preview=3, logger=None):
        self.fname_list = fname_list
        self.sep = sep
        self.all_strings = all_strings
        self.header_row = header_row
        self.skiprows=skiprows
        self.nrows_preview = nrows_preview
        self.logger = logger

    def read_csv(self, fname, is_preview=False, chunksize=None):
        cfg_dype = str if self.all_strings else None
        cfg_nrows = self.nrows_preview if is_preview else None
        return pd.read_csv(fname, dtype=cfg_dype, sep=self.sep, header=self.header_row, skiprows=self.skiprows, nrows=cfg_nrows, chunksize=chunksize)

    def read_csv_all(self, msg=None, is_preview=False, chunksize=None, cfg_col_sel=None, cfg_col_rename={}):
        dfl_all = []
        for fname in self.fname_list:
            if self.logger and msg:
                self.logger.send_log(msg+' '+ntpath.basename(fname),'ok')
            df=self.read_csv(fname, is_preview=is_preview, chunksize=chunksize)
            df['filename'] = ntpath.basename(fname)
            if cfg_col_sel:
                df = df.reindex(columns=['filename']+cfg_col_sel)
            df = df.rename(columns=cfg_col_rename)
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
        col_unique = list(set(col_all)-set(col_common))

        # find index in column list so can check order is correct
        df_col_present = {}
        for iFileName,iFileCol in col_files.items():
                df_col_present[iFileName]=[ntpath.basename(iFileName),]+[iCol in iFileCol for iCol in col_all]
                
        df_col_present = pd.DataFrame(df_col_present,index=['filename']+col_all).T
        df_col_present.index.names = ['file_path']
                    
        # find index in column list so can check order is correct
        df_col_order = {}
        for iFileName,iFileCol in col_files.items():
                df_col_order[iFileName]=[ntpath.basename(iFileName),]+[iFileCol.index(iCol) if iCol in iFileCol else np.nan for iCol in col_all]
        df_col_order = pd.DataFrame(df_col_order,index=['filename']+col_all).T

        col_preview = {'files_columns':col_files, 'columns_all':col_all, 'columns_common':col_common, 'columns_unique':col_unique, 'is_all_equal':columns_all_equal(dfl_all_col), 'df_columns_present':df_col_present, 'df_columns_order':df_col_order}
        self.col_preview = col_preview

        return col_preview


    def combine_preview(self, is_col_common = False):
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


    def combine(self, is_col_common = False, is_preview=False):
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
            self.logger.send_log('combining files','ok')

        if is_col_common:
            df_all = pd.concat(dfl_all,join='inner')
        else:
            df_all = pd.concat(dfl_all)
            
        self.df_all = df_all

        return df_all


class CombinerCSVAdvanced(object):

    def __init__(self, combiner, cfg_col_sel, cfg_col_rename={}):
        self.combiner = combiner
        self.cfg_col_sel = cfg_col_sel 
        self.cfg_col_rename = cfg_col_rename

    def combine_preview(self):
        df_all = self.combiner.read_csv_all(msg='reading preview file', is_preview=True, cfg_col_sel=self.cfg_col_sel, cfg_col_rename=self.cfg_col_rename)
        df_all = pd.concat(df_all)
        return df_all

    def combine_preview_save(self, fname_out):
        df_all_preview = self.combine_preview()
        df_all_preview.to_csv(fname_out,index=False)
        return True

    def combine(self):
        df_all = self.combiner.read_csv_all(msg='reading full file', cfg_col_sel=self.cfg_col_sel, cfg_col_rename=self.cfg_col_rename)
        df_all = pd.concat(df_all)
        return df_all


    def combine_save(self, fname_out):
        cfg_dype = str if self.combiner.all_strings else None
        cfg_col_sel = ['filename']+self.cfg_col_sel

        if not os.path.exists(os.path.dirname(fname_out)):
            os.makedirs(os.path.dirname(fname_out))
            
        fhandle = open(fname_out,'w')
        
        # write header
        df_all_header = pd.DataFrame(columns=cfg_col_sel)
        df_all_header.rename(columns=self.cfg_col_rename).to_csv(fhandle,header=True,index=False)
        # todo: what if file hasn't header
        
        for fname in self.combiner.fname_list:
            if self.combiner.logger:
                self.combiner.logger.send_log('processing '+ntpath.basename(fname),'ok')
            for df_chunk in self.combiner.read_csv(fname,chunksize=1e5):
                df_chunk['filename'] = ntpath.basename(fname)
                df_chunk = df_chunk.reindex(columns=cfg_col_sel) # todo: only reindex if need be
                df_chunk = df_chunk.rename(columns=self.cfg_col_rename) # todo: only rename if need be
                df_chunk.to_csv(fhandle,header=False,index=False)
                
        return True
        

