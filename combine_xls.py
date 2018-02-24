import os
import ntpath

import numpy as np
import pandas as pd

import openpyxl
import xlrd

from .helpers import *
from .helpers_ui import *


def check_valid_xls(fname_list):
    ext_list = file_extensions_get(fname_list)

    if not file_extensions_all_equal(ext_list):
        raise IOError('All file types and extensions have to be equal')

    if not(file_extensions_contains_xls(ext_list) or file_extensions_contains_xlsx(ext_list)):
        raise IOError('Only .xls, .xlsx files can be converted')

    return True


#******************************************************************
# sniffer
#******************************************************************

class XLSSniffer(object):

    def __init__(self, fname_list, logger=None):
        self.fname_list = fname_list
        self.logger = logger
        check_valid_xls(self.fname_list)
        self.scan()

    def scan(self):

        xls_sheets = {}
        for fname in self.fname_list:
            if self.logger:
                self.logger.send_log('sniffing sheets in '+ntpath.basename(fname),'ok')

            xls_fname = {}
            xls_fname['file_name'] = ntpath.basename(fname)
            if fname[-5:]=='.xlsx':
                fh = openpyxl.load_workbook(fname,read_only=True)
                xls_fname['sheets_names'] = fh.sheetnames
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

    def all_contain_sheet(self,sheet_name):
        return np.all([sheet_name in self.dict_xls_sheets[fname]['sheets_names'] for fname in self.fname_list])

    def all_have_idx(self,sheet_idx):
        return np.all([sheet_idx<=d['sheets_count'] for d in self.dict_xls_sheets])

    def all_same_count(self):
        return np.all([self.dict_xls_sheets[0]['sheets_count']==d['sheets_count'] for d in self.dict_xls_sheets])

    def all_same_names(self):
        return np.all([self.dict_xls_sheets[0]['sheets_names']==d['sheets_names'] for d in self.dict_xls_sheets])


#******************************************************************
# convertor
#******************************************************************
class XLStoCSVMultiFile(object):

    def __init__(self, fname_list, cfg_xls_sheets_sel_mode, cfg_xls_sheets_sel, logger=None):
        self.logger = logger
        self.set_files(fname_list)
        self.set_select_mode(cfg_xls_sheets_sel_mode, cfg_xls_sheets_sel)

    def set_files(self, fname_list):
        self.fname_list = fname_list
        self.xlsSniffer = XLSSniffer(fname_list)

    def set_select_mode(self, cfg_xls_sheets_sel_mode, cfg_xls_sheets_sel):

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


    def _convert_single(self, fname):
        if self.logger:
            self.logger.send_log('sniffing sheets in '+ntpath.basename(fname),'ok')

        fname_out = fname+'-'+str(self.cfg_xls_sheets_sel[fname])+'.csv'
        df = pd.read_excel(fname, sheetname=self.cfg_xls_sheets_sel[fname], dtype='str')
        df.to_csv(fname_out,index=False)

        return fname_out

    def convert_all(self):
        # todo: customize output dir

        # read files
        fnames_converted = []
        for fname in self.fname_list:
            fname_out = self._convert_single(fname)
            fnames_converted.append(fname_out)

        return fnames_converted


class XLStoCSVMultiSheet(object):

    def __init__(self, fname, logger=None):
        assert type(fname) is str
        self.logger = logger
        self.set_files(fname)

    def set_files(self, fname):
        self.fname = fname
        self.xlsSniffer = XLSSniffer([fname,])

    def _convert_single(self, fname):

        return fname_out

    def convert_all(self):
        # todo: customize output dir

        # read files
        fnames_converted = []
        for iSheet in self.xlsSniffer[self.fname]['sheet_name']:
            if self.logger:
                self.logger.send_log('sniffing sheets in '+ntpath.basename(fname),'ok')

            fname_out = fname+'-'+str(iSheet)+'.csv'
            df = pd.read_excel(fname, sheetname=iSheet, dtype='str')
            df.to_csv(fname_out,index=False)
            fnames_converted.append(fname_out)

        return fnames_converted

