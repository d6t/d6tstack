#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Finds CSV settings and Excel sheets in multiple files. Often needed as input for stacking

"""
import collections
import csv
import ntpath

import numpy as np
import pandas as pd
import openpyxl
import xlrd

from .helpers import check_valid_xls

#******************************************************************
# csv
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
    """
    
    Automatically detects settings needed to read csv files. SINGLE file only, for MULTI file use CSVSnifferList

    Args:
        fname (string): file path
        nlines (int): number of lines to sample from each file
        delims (string): possible delimiters, default ",;\t|"

    """

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
    """
    
    Automatically detects settings needed to read csv files. MULTI file use

    Args:
        fname_list (list): file names, eg ['a.csv','b.csv']
        nlines (int): number of lines to sample from each file
        delims (string): possible delimiters, default ',;\t|'

    """


    def __init__(self, fname_list, nlines = 10, delims=',;\t|'):
        self.cfg_fname_list = fname_list
        self.sniffers = [CSVSniffer(fname, nlines, delims) for fname in fname_list]

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

        # todo: propagate status of individual sniffers. instead of raising exception pass back status to get user input


#******************************************************************
# xls
#******************************************************************

class XLSSniffer(object):
    """
    
    Extracts available sheets from MULTIPLE Excel files and runs diagnostics

    Args:
        fname_list (list): file paths, eg ['dir/a.csv','dir/b.csv']
        logger (object): logger object with send_log(), optional

    """

    def __init__(self, fname_list, logger=None):
        if not fname_list:
            raise ValueError("Filename list should not be empty")
        self.fname_list = fname_list
        self.logger = logger
        check_valid_xls(self.fname_list)
        self.sniff()

    def sniff(self):
        """
        
        Executes sniffer

        Returns:
            boolean: True if everything ok. Results are accessible in ``.df_xls_sheets``

        """

        xls_sheets = {}
        for fname in self.fname_list:
            if self.logger:
                self.logger.send_log('sniffing sheets in '+ntpath.basename(fname),'ok')

            xls_fname = {}
            xls_fname['file_name'] = ntpath.basename(fname)
            if fname[-5:]=='.xlsx':
                fh = openpyxl.load_workbook(fname,read_only=True)
                xls_fname['sheets_names'] = fh.sheetnames
                fh.close()
                # todo: need to close file?
            elif fname[-4:]=='.xls':
                fh = xlrd.open_workbook(fname, on_demand=True)
                xls_fname['sheets_names'] = fh.sheet_names()
                fh.release_resources()
            else:
                raise IOError('Only .xls or .xlsx files can be combined')

            xls_fname['sheets_count'] = len(xls_fname['sheets_names'])
            xls_fname['sheets_idx'] = np.arange(xls_fname['sheets_count']).tolist()
            xls_sheets[fname] = xls_fname

            self.xls_sheets = xls_sheets

        df_xls_sheets = pd.DataFrame(xls_sheets).T
        df_xls_sheets.index.names = ['file_path']

        self.dict_xls_sheets = xls_sheets
        self.df_xls_sheets = df_xls_sheets

        return True

    def all_contain_sheetname(self,sheet_name):
        """        
        Check if all files contain a certain sheet

        Args:
            sheet_name (string): sheetname to check

        Returns:
            boolean: If true

        """
        return np.all([sheet_name in self.dict_xls_sheets[fname]['sheets_names'] for fname in self.fname_list])

    def all_have_idx(self,sheet_idx):
        """        
        Check if all files contain a certain index

        Args:
            sheet_idx (string): index to check

        Returns:
            boolean: If true

        """
        return np.all([sheet_idx<=(d['sheets_count']-1) for k,d in self.dict_xls_sheets.items()])

    def all_same_count(self):
        """        
        Check if all files contain the same number of sheets

        Args:
            sheet_idx (string): index to check

        Returns:
            boolean: If true

        """
        first_elem = next(iter(self.dict_xls_sheets.values()))
        return np.all([first_elem['sheets_count']==d['sheets_count'] for k,d in self.dict_xls_sheets.items()])

    def all_same_names(self):
        first_elem = next(iter(self.dict_xls_sheets.values()))
        return np.all([first_elem['sheets_names']==d['sheets_names'] for k,d in self.dict_xls_sheets.items()])


