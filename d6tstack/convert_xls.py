import warnings
import os.path

import numpy as np
import pandas as pd

import ntpath

import openpyxl
import xlrd

try:
    from openpyxl.utils.cell import coordinate_from_string
except:
    from openpyxl.utils import coordinate_from_string
from d6tstack.helpers import compare_pandas_versions, check_valid_xls

import d6tcollect
# d6tcollect.init(__name__)

#******************************************************************
# read_excel_advanced
#******************************************************************
def read_excel_advanced(fname, remove_blank_cols=True, remove_blank_rows=True, collapse_header=True,
                        header_xls_range=None, header_xls_start=None, header_xls_end=None,
                        is_preview=False, nrows_preview=3, **kwds):
    """
    Read Excel files to pandas dataframe with advanced options like set header ranges and remove blank columns and rows

    Args:
        fname (str): Excel file path
        remove_blank_cols (bool): remove blank columns
        remove_blank_rows (bool): remove blank rows
        collapse_header (bool): to convert multiline header to a single line string
        header_xls_range (string): range of headers in excel, eg: A4:B16
        header_xls_start (string): Starting cell of excel for header range, eg: A4
        header_xls_end (string): End cell of excel for header range, eg: B16
        is_preview (bool): Read only first `nrows_preview` lines
        nrows_preview (integer): Initial number of rows to be used for preview columns (default: 3)
        kwds (mixed): parameters for `pandas.read_excel()` to pass through

    Returns:
         df (dataframe): pandas dataframe

    Note:
        You can pass in any `pandas.read_excel()` parameters in particular `sheet_name`

    """
    header = []

    if header_xls_range:
        if not (header_xls_start and header_xls_end):
            header_xls_range = header_xls_range.split(':')
            header_xls_start, header_xls_end = header_xls_range
        else:
            raise ValueError('Parameter conflict. Can only pass header_xls_range or header_xls_start with header_xls_end')

    if header_xls_start and header_xls_end:
        if 'skiprows' in kwds or 'usecols' in kwds:
            raise ValueError('Parameter conflict. Cannot pass skiprows or usecols with header_xls')

        scol, srow = coordinate_from_string(header_xls_start)
        ecol, erow = coordinate_from_string(header_xls_end)

        # header, skiprows, usecols
        header = list(range(erow - srow + 1))
        usecols = scol + ":" + ecol
        skiprows = srow - 1

        if compare_pandas_versions(pd.__version__, "0.20.3") > 0:
            df = pd.read_excel(fname, header=header, skiprows=skiprows, usecols=usecols, **kwds)
        else:
            df = pd.read_excel(fname, header=header, skiprows=skiprows, parse_cols=usecols, **kwds)
    else:
        df = pd.read_excel(fname, **kwds)

    # remove blank cols and rows
    if remove_blank_cols:
        df = df.dropna(axis='columns', how='all')
    if remove_blank_rows:
        df = df.dropna(axis='rows', how='all')

    # todo: add df.reset_index() once no actual data in index

    # clean up header
    if collapse_header:
        if len(header) > 1:
            df.columns = [' '.join([s for s in col if not 'Unnamed' in s]).strip().replace("\n", ' ')
                          for col in df.columns.values]
            df = df.reset_index()
        else:
            df.rename(columns=lambda x: x.strip().replace("\n", ' '), inplace=True)

    # preview
    if is_preview:
        df = df.head(nrows_preview)

    return df


#******************************************************************
# XLSSniffer
#******************************************************************

class XLSSniffer(object, metaclass=d6tcollect.Collect):
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



#******************************************************************
# convertor
#******************************************************************
class XLStoBase(object, metaclass=d6tcollect.Collect):
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


class XLStoCSVMultiFile(XLStoBase, metaclass=d6tcollect.Collect):
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


class XLStoCSVMultiSheet(XLStoBase, metaclass=d6tcollect.Collect):
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
