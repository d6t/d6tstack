import os
import ntpath
import collections
import csv

import numpy as np
import pandas as pd

from .helpers import *
from .helpers_ui import *

#******************************************************************
# sniffer
#******************************************************************

def csv_count_rows(fname):
    def blocks(files, size=65536):
        while True:
            b = files.read(size)
            if not b: break
            yield b

    with open(fname) as f:
        nrows = sum(bl.count("\n") for bl in blocks(f))

    return nrows

class CSVSniffer(object):

    def __init__(self, fname, nlines = 10, delims=',;\t|'):
        self.cfg_fname = fname
        self.nrows = csv_count_rows(fname) # todo: check for file size, if large don't run this
        self.cfg_nlines = min(nlines,self.nrows) # read_lines() doesn't check EOF # todo: check 1% of file up to a max
        self.cfg_delims_pool = delims
        self.delim = None # delim used for the file
        self.csv_lines = None # top n lines read from file
        self.csv_lines_delim = None # detected delim for each line in file
        self.csv_rows = None # top n lines split usingn delim

    def read_nlines(self):
        # read top lines
        fhandle = open(self.cfg_fname)
        self.csv_lines = [fhandle.readline().rstrip() for _ in range(self.cfg_nlines)]
        fhandle.close()

    def scan_delim(self):
        if not self.csv_lines:
            self.read_nlines()

        # get delimiter for each line in file
        delims = []
        for line in self.csv_lines:
            try:
                csv_sniff = csv.Sniffer().sniff(line, self.cfg_delims_pool)
                delims.append(csv_sniff.delimiter)
            except:
                delims.append(None) # todo: able to catch exception more specifically?

        self.csv_lines_delim = delims

    def get_delim(self):
        if not self.csv_lines_delim:
            self.scan_delim()

        # all delimiters the same?
        if len(set(self.csv_lines_delim))>1:
            self.delim_is_consistent = False
            csv_delim_count = collections.Counter(self.csv_lines_delim)
            csv_delim = csv_delim_count.most_common(1)[0][0] # use the most common used delimeter
            # todo: rerun on cfg_csv_scan_topline**2 files in case there is a large # of header rows
        else:
            self.delim_is_consistent = True
            csv_delim = self.csv_lines_delim[0]

        if csv_delim==None:
            raise IOError('Could not determine a valid delimiter, pleaes check your files are .csv or .txt using one delimiter of %s' %(self.cfg_delims_pool))
        else:
            self.delim = csv_delim

        self.csv_rows = [s.split(self.delim) for s in self.csv_lines][self.count_skiprows():]
        if self.check_column_length_consistent():
            self.certainty = 'high'
        else:
            self.certainty = 'probable'

        return self.delim

    def check_column_length_consistent(self):
        # check if all rows have the same length. NB: this is just on the sample!
        if not self.csv_rows:
            self.get_delim()

        return len(set([len(row) for row in self.csv_rows]))==1

    def count_skiprows(self):
        # finds the number of rows to skip by finding the last line which doesn't use the selected delimiter
        if not self.delim:
            self.get_delim()

        if self.delim_is_consistent: # all delims the same so nothing to skip
            return 0

        l = [d != self.delim for d in self.csv_lines_delim]
        l = list(reversed(l))
        return len(l) - l.index(True)

    def has_header_inverse(self):
        # checks if head present if all columns in first row contain a letter
        if not self.csv_rows:
            self.get_delim()

        def is_number(s):
            try:
                float(s)
                return True
            except ValueError:
                return False

        self.is_all_rows_number_col = all([any([is_number(s) for s in row]) for row in self.csv_rows])

        '''
        self.row_distance = [distance.jaccard(self.csv_rows[0], self.csv_rows[i]) for i in range(1,len(self.csv_rows))]

        iqr_low, iqr_high = np.percentile(self.row_distance[1:], [5, 95])
        is_first_row_different = not(iqr_low <= self.row_distance[0] <= iqr_high)
        '''

    def has_header(self):
        # more likely than not to contain headers so have to prove no header present
        self.has_header_inverse()
        return not self.is_all_rows_number_col

class CSVSnifferList(object):

    def __init__(self, fname_list):
        self.cfg_fname_list = fname_list
        self.sniffers = [CSVSniffer(fname) for fname in fname_list]

    def get_all(self, fun_name, msg_error):
        val = []
        for sniffer in self.sniffers:
            func = getattr(sniffer, fun_name)
            val.append(func())

        if len(set(val))>1:
            raise NotImplementedError(msg_error+' Make sure all files have the same format')
            # todo: want to raise an exception here...? or just use whatever got detected for each file?
        else:
            return val[0]

    def get_delim(self):
        return self.get_all('get_delim','Inconsistent delimiters detected!')

    def count_skiprows(self):
        return self.get_all('count_skiprows','Inconsistent skiprows detected!')

    def has_header(self):
        return self.get_all('has_header','Inconsistent header setting detected!')

        # todo: check for column consistency
        # todo: propagate status of individual sniffers. instead of raising exception pass back status to get user input


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

    def __init__(self, fname_list, sep=',', all_strings = False, header_row = 0, skiprows=0, nrows_preview=5, logger=None):
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
        
        dfl_all = self.read_csv_all(msg='scanning colums of', is_preview=True)

        dfl_all_col = [df.columns.tolist() for df in dfl_all]
        [df.remove('filename') for df in dfl_all_col]
        col_files = dict(zip(self.fname_list, dfl_all_col))
        col_common = list_common(list(col_files.values()))
        col_all = list_unique(list(col_files.values()))

        df_col = {}
        for iFileName,iFileCol in col_files.items():
                df_col[iFileName]=[ntpath.basename(iFileName),]+[iCol in iFileCol for iCol in col_all]
                
        df_col = pd.DataFrame(df_col,index=['filename']+col_all).T
        df_col.index.names = ['file_path']
        df_col_json = df_col.reset_index(drop=True).to_json(orient='records')
                    
        return col_files, col_all, col_common, columns_all_equal(dfl_all_col), df_col, df_col_json


    def combine_preview(self, cfg_col_mode='all'):
        return self.combine(cfg_col_mode, is_preview=True)


    def combine(self, cfg_col_mode='all', is_preview=False):
        dfl_all = self.read_csv_all('reading full file')

        if self.logger:
            self.logger.send_log('combining files','ok')

        if cfg_col_mode == 'all':
            df_all = pd.concat(dfl_all)
        elif cfg_col_mode == 'common':
            df_all = pd.concat(dfl_all,join='inner')
        else:
            raise ValueError('invalid columns_select_mode')
            
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
        df_all_header.to_csv(fhandle,header=True,index=False)
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
        

