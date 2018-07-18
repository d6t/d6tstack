import warnings
import os.path
import ntpath

import numpy as np
import pandas as pd

from .sniffer import XLSSniffer
from .utils import read_excel_advanced

#******************************************************************
# convertor
#******************************************************************
class XLStoBase(object):
    def __init__(self, if_exists='skip', output_dir=None, logger=None):
        """

        Base class for converting Excel files

        Args:
            if_exists (str): Possible values: skip and replace, default: skip, optional
            output_dir (str): If present, file is saved in given directory, optional
            logger (object): logger object with send_log('msg','status'), optional

        """

        if if_exists not in ['skip', 'replace']:
            raise ValueError("Possible value of 'if_exists' are 'skip' and 'replace'")
        self.logger = logger
        self.if_exists = if_exists
        self.output_dir = output_dir
        if self.output_dir:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

    def _get_output_filename(self, fname):
        if self.output_dir:
            basename = os.path.basename(fname)
            fname_out = os.path.join(self.output_dir, basename)
        else:
            fname_out = fname
        is_skip = (self.if_exists == 'skip' and os.path.isfile(fname_out))
        return fname_out, is_skip

    def convert_single(self, fname, sheet_name, **kwds):
        """

        Converts single file

        Args:
            fname: path to file
            sheet_name (str): optional sheet_name to override global `cfg_xls_sheets_sel`
            Same as `d6tstack.utils.read_excel_advanced()`

        Returns:
            list: output file names

        """

        if self.logger:
            msg = 'converting file: '+ntpath.basename(fname)+' | sheet: '
            if hasattr(self, 'cfg_xls_sheets_sel'):
                msg += str(self.cfg_xls_sheets_sel[fname])
            self.logger.send_log(msg,'ok')

        fname_out = fname + '-' + str(sheet_name) + '.csv'
        fname_out, is_skip = self._get_output_filename(fname_out)
        if not is_skip:
            df = read_excel_advanced(fname, sheet_name=sheet_name, **kwds)
            df.to_csv(fname_out, index=False)
        else:
            warnings.warn('File %s exists, skipping' %fname)

        return fname_out


class XLStoCSVMultiFile(XLStoBase):
    """
    
    Converts xls|xlsx files to csv files. Selects a SINGLE SHEET from each file. To extract MULTIPLE SHEETS from a file use XLStoCSVMultiSheet

    Args:
        fname_list (list): file paths, eg ['dir/a.csv','dir/b.csv']
        cfg_xls_sheets_sel_mode (string): mode to select tabs

            * ``name``: select by name, provide name for each file, can customize by file
            * ``name_global``: select by name, one name for all files
            * ``idx``: select by index, provide index for each file, can customize by file
            * ``idx_global``: select by index, one index for all files

        cfg_xls_sheets_sel (dict): values to select tabs `{'filename':'value'}`
        output_dir (str): If present, file is saved in given directory, optional
        if_exists (str): Possible values: skip and replace, default: skip, optional
        logger (object): logger object with send_log('msg','status'), optional

    """

    def __init__(self, fname_list, cfg_xls_sheets_sel_mode='idx_global', cfg_xls_sheets_sel=0,
                 output_dir=None, if_exists='skip', logger=None):
        super().__init__(if_exists, output_dir, logger)
        if not fname_list:
            raise ValueError("Filename list should not be empty")
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

    def convert_all(self, **kwds):
        """
        
        Converts all files

        Args:
            Any parameters for `d6tstack.utils.read_excel_advanced()`

        Returns: 
            list: output file names
        """

        fnames_converted = []
        for fname in self.fname_list:
            fname_out = self.convert_single(fname, self.cfg_xls_sheets_sel[fname], **kwds)
            fnames_converted.append(fname_out)

        return fnames_converted


class XLStoCSVMultiSheet(XLStoBase):
    """
    
    Converts ALL SHEETS from a SINGLE xls|xlsx files to separate csv files

    Args:
        fname (string): file path
        sheet_names (list): list of int or str. If not given, will convert all sheets in the file
        output_dir (str): If present, file is saved in given directory, optional
        if_exists (str): Possible values: skip and replace, default: skip, optional
        logger (object): logger object with send_log('msg','status'), optional

    """

    def __init__(self, fname, sheet_names=None, output_dir=None, if_exists='skip', logger=None):
        super().__init__(if_exists, output_dir, logger)
        self.fname = fname


        if sheet_names:
            if not isinstance(sheet_names, (list,str)):
                raise ValueError('sheet_names needs to be a list')
            self.sheet_names = sheet_names
        else:
            self.xlsSniffer = XLSSniffer([fname, ])
            self.sheet_names = self.xlsSniffer.xls_sheets[self.fname]['sheets_names']

    def convert_single(self, sheet_name, **kwds):
        """

        Converts all files

        Args:
            sheet_name (str): Excel sheet
            Any parameters for `d6tstack.utils.read_excel_advanced()`

        Returns:
            str: output file name
        """
        return super().convert_single(self.fname, sheet_name, **kwds)

    def convert_all(self, **kwds):
        """

        Converts all files

        Args:
            Any parameters for `d6tstack.utils.read_excel_advanced()`

        Returns:
            list: output file names
        """

        fnames_converted = []
        for iSheet in self.sheet_names:
            fname_out = self.convert_single(iSheet, **kwds)
            fnames_converted.append(fname_out)

        return fnames_converted
