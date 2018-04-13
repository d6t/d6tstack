import os
import ntpath

import numpy as np
import pandas as pd

import openpyxl
import xlrd

from .helpers import check_valid_xls
from .sniffer import XLSSniffer
from .read_excel_adv import read_excel_advanced

#******************************************************************
# convertor
#******************************************************************
class XLStoCSVMultiFile(object):
    """
    
    Converts xls|xlsx files to csv files. Selects a SINGLE SHEET from each file. To extract MULTIPLE SHEETS from a file use XLStoCSVMultiSheet

    Args:
        fname_list (list): file paths, eg ['dir/a.csv','dir/b.csv']
        cfg_xls_sheets_sel_mode (string): mode to select tabs

            * ``name``: select by name, provide name for each file, can customize by file
            * ``name_global``: select by name, one name for all files
            * ``idx``: select by index, provide index for each file, can customize by file
            * ``idx_global``: select by index, one index for all files

        cfg_xls_sheets_sel (list): values to select tabs **NEEDS TO BE IN THE SAME ORDER AS `fname_list`**
        logger (object): logger object with send_log(), optional

    """


    def __init__(self, fname_list, cfg_xls_sheets_sel_mode, cfg_xls_sheets_sel,
                 logger=None):
        self.logger = logger
        self.set_files(fname_list)
        self.set_select_mode(cfg_xls_sheets_sel_mode, cfg_xls_sheets_sel)

    def set_files(self, fname_list):
        """
        
        Update input files. You will also need to update sheet selection with ``.set_select_mode()``.

        Args:
            fname_list (list): see class description for details

        """
        self.fname_list = fname_list
        self.xlsSniffer = XLSSniffer(fname_list)

    def set_select_mode(self, cfg_xls_sheets_sel_mode, cfg_xls_sheets_sel):
        """
        
        Update sheet selection values

        Args:
            cfg_xls_sheets_sel_mode (string): see class description for details
            cfg_xls_sheets_sel (list): see class description for details

        """

        assert cfg_xls_sheets_sel_mode in ['name','idx','name_global','idx_global']
        sheets = self.xlsSniffer.dict_xls_sheets

        if cfg_xls_sheets_sel_mode=='name_global':
            cfg_xls_sheets_sel_mode = 'name'
            cfg_xls_sheets_sel = dict(zip(self.fname_list,[cfg_xls_sheets_sel]*len(self.fname_list)))
        elif cfg_xls_sheets_sel_mode=='idx_global':
            cfg_xls_sheets_sel_mode = 'idx'
            cfg_xls_sheets_sel = dict(zip(self.fname_list,[cfg_xls_sheets_sel]*len(self.fname_list)))

        if not set(cfg_xls_sheets_sel.keys())==set(sheets.keys()):
            raise ValueError('Need to select a sheet from every file')

        # check given selection actually present in files
        if cfg_xls_sheets_sel_mode=='name':
            if not np.all([cfg_xls_sheets_sel[fname] in sheets[fname]['sheets_names'] for fname in self.fname_list]):
                raise ValueError('Invalid sheet name selected in one of the files')
                # todo show which file is mismatched
        elif cfg_xls_sheets_sel_mode=='idx':
            if not np.all([cfg_xls_sheets_sel[fname] <= sheets[fname]['sheets_count'] for fname in self.fname_list]):
                raise ValueError('Invalid index selected in one of the files')
                # todo show which file is mismatched
        else:
            raise ValueError('Invalid xls_sheets_mode')

        self.cfg_xls_sheets_sel_mode = cfg_xls_sheets_sel_mode
        self.cfg_xls_sheets_sel = cfg_xls_sheets_sel

    def _convert_single(self, fname, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                        header_xls_range=None, header_xls_start=None, header_xls_end=None):
        if self.logger:
            self.logger.send_log('converting file: '+ntpath.basename(fname)+' | sheet: '+ str(self.cfg_xls_sheets_sel[fname]),'ok')

        fname_out = fname+'-'+str(self.cfg_xls_sheets_sel[fname])+'.csv'
        df = read_excel_advanced(fname, remove_blank_cols=remove_blank_cols, remove_blank_rows=remove_blank_rows,
                                 collapse_header=collapse_header, header_xls_range=header_xls_range,
                                 header_xls_start=header_xls_start, header_xls_end=header_xls_end,
                                 sheet_name=self.cfg_xls_sheets_sel[fname], dtype='str')
        df.to_csv(fname_out,index=False)

        return fname_out

    def convert_all(self, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                 header_xls_range=None, header_xls_start=None, header_xls_end=None):
        """
        
        Executes conversion. Writes to the same path as file and appends .csv to filename.

        Returns: 
            list: output file names
        """

        # todo: customize output dir. customize output filename

        # read files
        fnames_converted = []
        for fname in self.fname_list:
            fname_out = self._convert_single(fname, remove_blank_cols=remove_blank_cols,
                                             remove_blank_rows=remove_blank_rows, collapse_header=collapse_header,
                                             header_xls_range=header_xls_range, header_xls_start=header_xls_start,
                                             header_xls_end=header_xls_end)
            fnames_converted.append(fname_out)

        return fnames_converted


class XLStoCSVMultiSheet(object):
    """
    
    Converts ALL SHEETS from a SINGLE xls|xlsx files to separate csv files

    Args:
        fname (string): file path
        logger (object): logger object with send_log()

    """

    def __init__(self, fname, sheet_names=None, logger=None):
        assert type(fname) is str
        self.logger = logger
        self.set_files(fname)
        assert sheet_names is None or isinstance(sheet_names, list)
        self.sheet_names = sheet_names

    def set_files(self, fname):
        self.fname = fname
        self.xlsSniffer = XLSSniffer([fname,])

    def _convert_single(self, fname):

        return fname

    def convert_all(self, remove_blank_cols=False, remove_blank_rows=False, collapse_header=False,
                    header_xls_range=None, header_xls_start=None, header_xls_end=None):
        # todo: customize output dir

        # read files
        fnames_converted = []
        for iSheet in self.xlsSniffer.xls_sheets[self.fname]['sheets_names']:
            if not self.sheet_names or iSheet in self.sheet_names:
                if self.logger:
                    self.logger.send_log('sniffing sheets in '+ntpath.basename(self.fname),'ok')

                fname_out = self.fname+'-'+str(iSheet)+'.csv'
                df = pd.read_excel(self.fname, remove_blank_cols=remove_blank_cols, remove_blank_rows=remove_blank_rows,
                                   collapse_header=collapse_header, header_xls_range=header_xls_range,
                                   header_xls_start=header_xls_start, header_xls_end=header_xls_end,
                                   sheet_name=iSheet, dtype='str')
                df.to_csv(fname_out,index=False)
                fnames_converted.append(fname_out)
            else:
                if self.logger:
                    self.logger.send_log('Ignoring sheet: ' + iSheet, 'ok')

        return fnames_converted
