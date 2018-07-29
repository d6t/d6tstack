import numpy as np
import pandas as pd
import warnings
from sqlalchemy.engine import create_engine

from .sniffer import CSVSnifferList
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


def apply_select_rename(dfg, columns_select, columns_rename):

    if columns_rename:
        # check no naming conflicts
        columns_select2 = [columns_rename[k] if k in columns_rename.keys() else k for k in dfg.columns.tolist()]
        df_rename_count = collections.Counter(columns_select2)
        if df_rename_count and max(df_rename_count.values()) > 1:  # would the rename create naming conflict?
            warnings.warn('Renaming conflict: {}'.format([(k,v) for k,v in df_rename_count.items() if v>1]), UserWarning)
            while df_rename_count and max(df_rename_count.values())>1:
                # remove key value pair causing conflict
                conflicting_keys = [i for i,j in df_rename_count.items() if j>1]
                columns_rename = {k:v for k,v in columns_rename.items() if k in conflicting_keys}
                columns_select2 = [columns_rename[k] if k in columns_rename.keys() else k for k in dfg.columns.tolist()]
                df_rename_count = collections.Counter(columns_select2)
        dfg = dfg.rename(columns=columns_rename)
    if columns_select:
        if columns_rename:
            columns_select2 = list(dict.fromkeys([columns_rename[k] if k in columns_rename.keys() else k for k in columns_select])) # set of columns after rename
        else:
            columns_select2 = columns_select
        dfg = dfg.reindex(columns=columns_select2)

    return dfg


def create_sql_connection(cnxn_string):
    engine = create_engine(cnxn_string)
    connection = engine.connect()
    connection.dialect.supports_multivalues_insert = True
    return connection


def convert_to_sql(df, connection, table_name, if_exists='replace', chunksize=5000):
    df.to_sql(table_name, connection, schema=None, if_exists=if_exists, index=True, index_label=None,
              chunksize=chunksize, dtype=None)
    return True


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
        add_filename (bool): add filename column to output data frame. If `False`, will not add column.
        columns_select (list): list of column names to keep
        columns_rename (dict): dict of columns to rename `{'name_old':'name_new'}
        logger (object): logger object with send_log()

    """

    def __init__(self, fname_list, sep=',', has_header = True, all_strings=False, nrows_preview=3, read_csv_params=None,
                 add_filename=True, columns_select=None, columns_rename=None, logger=None):
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
        self.add_filename = add_filename
        self.columns_select = columns_select
        self.columns_rename = columns_rename

        if not self.columns_select:
            self.columns_select = []
        else:
            if max(collections.Counter(columns_select).values())>1:
                raise ValueError('Duplicate entries in columns_select')

        if not self.columns_rename:
            self.columns_rename = {}

    def read_csv(self, fname, is_preview=False, chunksize=None):
        cfg_dype = str if self.all_strings else None
        cfg_nrows = self.nrows_preview if is_preview else None
        return pd.read_csv(fname, dtype=cfg_dype, nrows=cfg_nrows, chunksize=chunksize,
                           **self.read_csv_params)

    def read_csv_all(self, msg=None, is_preview=False, chunksize=None, columns_select=None,
                     columns_rename=None):
        dfl_all = []
        if not columns_select:
            columns_select = []
        if not columns_rename:
            columns_rename = {}
        for fname in self.fname_list:
            if self.logger and msg:
                self.logger.send_log(msg + ' ' + ntpath.basename(fname), 'ok')
            df = self.read_csv(fname, is_preview=is_preview, chunksize=chunksize)
            if columns_select or columns_rename:
                df = apply_select_rename(df, columns_select, columns_rename)
            if self.add_filename:
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
        if self.add_filename:
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
        
        Combines all files. This is in-memory. For out-of-core use `combine_save()`

        Args:
            is_col_common (bool): keep only common columns? If `false` returns all columns filled with nans
            is_preview (bool): read only self.nrows_preview top rows

        Returns:
            df_all (dataframe): pandas dataframe with combined data from all files

        """

        dfl_all = self.read_csv_all('reading full file', is_preview=is_preview, columns_select=self.columns_select,
                                    columns_rename=self.columns_rename)

        if self.logger:
            self.logger.send_log('combining files', 'ok')

        if is_col_common:
            df_all = pd.concat(dfl_all, join='inner', sort=False)
        else:
            df_all = pd.concat(dfl_all, sort=False)

        self.df_all = df_all

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

    def get_output_filename(self, fname, suffix, parquet_output=False):
        basename = os.path.basename(fname)
        name_with_ext = os.path.splitext(basename)
        new_name = name_with_ext[0] + suffix
        if parquet_output:
            new_name += ".parquet"
        elif len(name_with_ext) == 2:
            new_name += name_with_ext[1]
        return new_name

    def create_output_dir(self, output_dir):
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def get_columns_for_save(self, is_col_common=False):
        if self.columns_select:
            # set of columns after rename
            columns_select2 = list(collections.OrderedDict.fromkeys([self.columns_rename[k]
                                                                  if k in self.columns_rename.keys() else k
                                                                  for k in self.columns_select]))

            return columns_select2
        else:
            self._preview_available()
            import copy
            columns = copy.deepcopy(self.col_preview['columns_common'] if is_col_common
                                    else self.col_preview['columns_all'])
            if self.add_filename:
                columns += ['filename', ]
            return columns

    def save_files(self, columns, out_filename=None, output_dir=None, suffix='-matched', overwrite=False, chunksize=1e10,
                   columns_select2=None, parquet_output=False):
        if parquet_output:
            import pyarrow as pa
            import pyarrow.parquet as pq
        df_all_header = pd.DataFrame(columns=columns)
        if out_filename and not overwrite and os.path.isfile(out_filename):
            warnings.warn("File already exists. Please pass overwrite=True for overwriting")
            return True
        if out_filename:
            if not parquet_output:
                fhandle = open(out_filename, 'w')
                df_all_header.to_csv(fhandle, header=True, index=False)
        first = True
        for fname in self.fname_list:
            if self.logger:
                self.logger.send_log('processing ' + ntpath.basename(fname), 'ok')

            new_name = self.get_output_filename(fname, suffix, parquet_output=parquet_output)
            if output_dir:
                fname_out = os.path.join(output_dir, new_name)
            else:
                fname_out = os.path.join(os.path.dirname(fname), new_name)
            if not out_filename and not overwrite and os.path.isfile(fname_out):
                warnings.warn("File already exists. Please pass overwrite=True for overwriting")
            else:
                if not out_filename:
                    if not parquet_output:
                        fhandle = open(fname_out, 'w')
                        df_all_header.to_csv(fhandle, header=True, index=False)
                    first = True
                for df_chunk in self.read_csv(fname, chunksize=chunksize):
                    if columns_select2 or self.columns_rename:
                        df_chunk = apply_select_rename(df_chunk, columns_select2, self.columns_rename)
                    if self.add_filename:
                        df_chunk['filename'] = ntpath.basename(new_name)
                    if parquet_output:
                        table = pa.Table.from_pandas(df_chunk)
                        if first:
                            if out_filename:
                                pqwriter = pq.ParquetWriter(out_filename, table.schema)
                            else:
                                pqwriter = pq.ParquetWriter(fname_out, table.schema)
                            first = False
                        pqwriter.write_table(table)
                    else:
                        df_chunk.to_csv(fhandle, header=False, index=False)
            if not out_filename and parquet_output:
                pqwriter.close()
        if out_filename and parquet_output:
            pqwriter.close()

        return True

    def align_save(self, output_dir=None, suffix='-matched', overwrite=False, chunksize=1e10,
                   is_col_common=False, parquet_output=False):
        """

        Save matched columns data directly to CSV for each of the files.

        Args:
            output_dir (str): output directory to save, default input file directory, optional
            suffix (str): suffix to add to end of screen to input filename to create output file name, optional
            overwrite (bool): overwrite file if exists, default True, optional
            is_col_common (bool): Use common columns else all columns, default False, optional

        """
        columns_select2 = self.get_columns_for_save(is_col_common=is_col_common)

        columns = columns_select2
        if self.add_filename and self.columns_select:
            columns += ['filename', ]

        return self.save_files(columns, output_dir=output_dir, suffix=suffix, overwrite=overwrite,
                               chunksize=chunksize, columns_select2=columns, parquet_output=parquet_output)

    def combine_save(self, fname_out, chunksize=1e10, is_col_common=False, parquet_output=False, overwrite=True):
        """

        Save combined data directly to CSV. This implements out-of-core combine functionality to combine large files. For in-memory use `combine()`

        Args:
            fname_out (str): filename

        """
        columns_select2 = self.get_columns_for_save(is_col_common=is_col_common)

        columns = columns_select2
        if self.add_filename and self.columns_select:
            columns += ['filename', ]

        self.create_output_dir(os.path.dirname(fname_out))

        return self.save_files(columns, out_filename=fname_out, chunksize=chunksize, columns_select2=columns_select2,
                               overwrite=overwrite, parquet_output=parquet_output)

    def to_sql(self, cnxn_string, table_name, is_col_common=False, is_preview=False,
               if_exists='replace', chunksize=5000):
        """

            Save combined files to sql.

            Args:
                cnxn_string (str): connection string to connect to database
                table_name (str): table name to be used to store the data to database
                is_col_common (bool): Use common columns else all columns, default False, optional
                is_preview (bool): read only self.nrows_preview top rows
                if_exists (str): replace or append to existing table, optional
                chunksize (int): Number of rows to be inserted to table at one time.
        """
        df = self.combine(is_col_common=is_col_common, is_preview=is_preview)
        connection = create_sql_connection(cnxn_string)
        convert_to_sql(df, connection, table_name, if_exists=if_exists, chunksize=chunksize)
        connection.close()
        return True

    def to_sql_stream(self, cnxn_string, table_name, if_exists='replace',
                      chunksize=1e10, sql_chunksize=5000, is_col_common=False):
        """

            Save combined large files in chunks to sql.

            Args:
                cnxn_string (str): connection string to connect to database
                table_name (str): table name to be used to store the data to database
                is_col_common (bool): Use common columns else all columns, default False, optional
                is_preview (bool): read only self.nrows_preview top rows
                if_exists (str): replace or append to existing table, optional
                chunksize (int): Number of lines to be used to extract from file each time.
                sql_chunksize (int): Number of rows to be inserted to table at one time.
        """
        columns_select = self.columns_select
        if not columns_select:
            columns_select = self.get_columns_for_save(is_col_common=is_col_common)
        first_time = True
        connection = create_sql_connection(cnxn_string)
        for fname in self.fname_list:
            if self.logger:
                self.logger.send_log('processing ' + ntpath.basename(fname), 'ok')
            for df_chunk in self.read_csv(fname, chunksize=chunksize):
                if columns_select or self.columns_rename:
                    df_chunk = apply_select_rename(df_chunk, columns_select, self.columns_rename)
                if self.add_filename:
                    df_chunk['filename'] = ntpath.basename(fname)
                if first_time:
                    if_exists = if_exists
                    first_time = False
                else:
                    if_exists = 'append'
                convert_to_sql(df_chunk, connection, table_name, if_exists=if_exists,
                               chunksize=sql_chunksize)
        connection.close()
        return True

    def convert_to_csv_parquet(self, out_filename=None, separate_files=True, output_dir=None, suffix='-matched',
                               is_col_common=False, overwrite=False, streaming=True, chunksize=1e10,
                               parquet_output=False):
        if separate_files:
            self.align_save(output_dir=output_dir, suffix=suffix, overwrite=overwrite, is_col_common=is_col_common,
                            chunksize=chunksize, parquet_output=parquet_output)
        elif streaming and out_filename:
            self.combine_save(out_filename, chunksize=chunksize, parquet_output=parquet_output, overwrite=overwrite)
        elif out_filename:
            df = self.combine(is_col_common=is_col_common)
            if parquet_output:
                import pyarrow as pa
                import pyarrow.parquet as pq
                table = pa.Table.from_pandas(df)
                pq.write_table(table, out_filename)
            else:
                fhandle = open(out_filename, 'w')
                df.to_csv(fhandle, header=True, index=False)
        else:
            raise ValueError("out_filename is mandatory when streaming")

    def to_csv(self, out_filename=None, separate_files=True, output_dir=None, suffix='-matched',
               is_col_common=False, overwrite=False, streaming=False, chunksize=1e10):
        """

        Convert the files to combined csv or separate csv after aligning the columns

        Args:
            out_filename (str): when combining this is mandatory
            separate_files (bool): To decide whether combine files or save separately, default True
            output_dir (str): output directory to save for separate files, default input file directory, optional
            suffix (str): suffix to add to end of screen to input filename to create output file name, optional
            overwrite (bool): overwrite file if exists, default True, optional
            chunksize (int): chunksize to be used for writing large files in chunks

        """

        self.convert_to_csv_parquet(out_filename=out_filename, separate_files=separate_files, output_dir=output_dir,
                                    is_col_common=is_col_common, suffix=suffix, overwrite=overwrite,
                                    streaming=streaming, chunksize=chunksize)

    def to_parquet(self, out_filename=None, separate_files=True, output_dir=None, suffix='-matched',
                   is_col_common=False, overwrite=False, streaming=False, chunksize=1e10):
        """

        Convert the files to combined csv or separate csv after aligning the columns

        Args:
            out_filename (str): when combining this is mandatory
            separate_files (bool): convert to csv after aligning columns (without combining)
            output_dir (str): output directory to save for separate files, default input file directory, optional
            suffix (str): suffix to add to end of screen to input filename to create output file name, optional
            overwrite (bool): overwrite file if exists, default True, optional
            chunksize (int): chunksize to be used for writing large files in chunks

        """
        self.convert_to_csv_parquet(out_filename=out_filename, separate_files=separate_files, output_dir=output_dir,
                                    suffix=suffix, overwrite=overwrite, streaming=streaming, chunksize=chunksize,
                                    is_col_common=is_col_common, parquet_output=True)
