import numpy as np
import pandas as pd
pd.set_option('display.expand_frame_repr', False)
from scipy.stats import mode
import warnings
import ntpath, pathlib
import copy
import itertools
import os

import d6tcollect
# d6tcollect.init(__name__)

from .helpers import *
from .utils import PrintLogger


# ******************************************************************
# helpers
# ******************************************************************
def _dfconact(df):
    return pd.concat(itertools.chain.from_iterable(df), sort=False, copy=False, join='inner', ignore_index=True)

def _direxists(fname, logger):
    fdir = os.path.dirname(fname)
    if fdir and not os.path.exists(fdir):
        if logger:
            logger.send_log('creating ' + fdir, 'ok')
        os.makedirs(fdir)
    return True

# ******************************************************************
# combiner
# ******************************************************************

class CombinerCSV(object, metaclass=d6tcollect.Collect):
    """    
    Core combiner class. Sniffs columns, generates preview, combines aka stacks to various output formats.

    Args:
        fname_list (list): file names, eg ['a.csv','b.csv']
        sep (string): CSV delimiter, see pandas.read_csv()
        has_header (boolean): data has header row 
        nrows_preview (int): number of rows in preview
        chunksize (int): number of rows to read into memory while processing, see pandas.read_csv()
        read_csv_params (dict): additional parameters to pass to pandas.read_csv()
        columns_select (list): list of column names to keep
        columns_select_common (bool): keep only common columns. Use this instead of `columns_select`
        columns_rename (dict): dict of columns to rename `{'name_old':'name_new'}
        add_filename (bool): add filename column to output data frame. If `False`, will not add column.
        apply_after_read (function): function to apply after reading each file. needs to return a dataframe
        log (bool): send logs to logger
        logger (object): logger object with `send_log()`

    """

    def __init__(self, fname_list, sep=',', nrows_preview=3, chunksize=1e6, read_csv_params=None,
                 columns_select=None, columns_select_common=False, columns_rename=None, add_filename=True,
                 apply_after_read=None, log=True, logger=None):
        if not fname_list:
            raise ValueError("Filename list should not be empty")
        self.fname_list = np.sort(fname_list)
        self.nrows_preview = nrows_preview
        self.read_csv_params = read_csv_params
        if not self.read_csv_params:
            self.read_csv_params = {}
        if not 'sep' in self.read_csv_params:
            self.read_csv_params['sep'] = sep
        if not 'chunksize' in self.read_csv_params:
            self.read_csv_params['chunksize'] = chunksize
        self.logger = logger
        if not logger and log:
            self.logger = PrintLogger()
        if not log:
            self.logger = None
        self.sniff_results = None
        self.add_filename = add_filename
        self.columns_select = columns_select
        self.columns_select_common = columns_select_common
        if columns_select and columns_select_common:
            warnings.warn('columns_select will override columns_select_common, pick either one')
        self.columns_rename = columns_rename
        self._columns_reindex = None
        self._columns_rename_dict = None
        self.apply_after_read = apply_after_read

        self.df_combine_preview = None

        if self.columns_select:
            if max(collections.Counter(columns_select).values())>1:
                raise ValueError('Duplicate entries in columns_select')

    def _read_csv_yield(self, fname, read_csv_params):
        self._columns_reindex_available()
        dfs = pd.read_csv(fname, **read_csv_params)
        for dfc in dfs:
            if self.columns_rename and self._columns_rename_dict[fname]:
                dfc = dfc.rename(columns=self._columns_rename_dict[fname])

            dfc = dfc.reindex(columns=self._columns_reindex)
            if self.apply_after_read:
                dfc = self.apply_after_read(dfc)
            if self.add_filename:
                dfc['filepath'] = fname
                dfc['filename'] = ntpath.basename(fname)
            yield dfc

    def sniff_columns(self):

        """
        
        Checks column consistency by reading top nrows in all files. It checks both presence and order of columns in all files

        Returns:
            dict: results dictionary with
                files_columns (dict): dictionary with information, keys = filename, value = list of columns in file
                columns_all (list): all columns in files
                columns_common (list): only columns present in every file
                is_all_equal (boolean): all files equal in all files?
                df_columns_present (dataframe): which columns are present in which file?
                df_columns_order (dataframe): where in the file is the column?

        """

        if self.logger:
            self.logger.send_log('sniffing columns', 'ok')

        read_csv_params = copy.deepcopy(self.read_csv_params)
        read_csv_params['dtype'] = str
        read_csv_params['nrows'] = self.nrows_preview
        read_csv_params['chunksize'] = None

        # read nrows of every file
        self.dfl_all = []
        for fname in self.fname_list:
            # todo: make sure no nrows param in self.read_csv_params
            df = pd.read_csv(fname, **read_csv_params)
            self.dfl_all.append(df)

        # process columns
        dfl_all_col = [df.columns.tolist() for df in self.dfl_all]
        col_files = dict(zip(self.fname_list, dfl_all_col))
        col_common = list_common(list(col_files.values()))
        col_all = list_unique(list(col_files.values()))

        # find index in column list so can check order is correct
        df_col_present = {}
        for iFileName, iFileCol in col_files.items():
            df_col_present[iFileName] = [iCol in iFileCol for iCol in col_all]

        df_col_present = pd.DataFrame(df_col_present, index=col_all).T
        df_col_present.index.names = ['file_path']

        # find index in column list so can check order is correct
        df_col_idx = {}
        for iFileName, iFileCol in col_files.items():
            df_col_idx[iFileName] = [iFileCol.index(iCol) if iCol in iFileCol else np.nan for iCol in col_all]
        df_col_idx = pd.DataFrame(df_col_idx, index=col_all).T

        # order columns by where they appear in file
        m=mode(df_col_idx,axis=0)
        df_col_pos = pd.DataFrame({'o':m[0][0],'c':m[1][0]},index=df_col_idx.columns)
        df_col_pos = df_col_pos.sort_values(['o','c'])
        df_col_pos['iscommon']=df_col_pos.index.isin(col_common)


        # reorder by position
        col_all = df_col_pos.index.values.tolist()
        col_common = df_col_pos[df_col_pos['iscommon']].index.values.tolist()
        col_unique = df_col_pos[~df_col_pos['iscommon']].index.values.tolist()
        df_col_present = df_col_present[col_all]
        df_col_idx = df_col_idx[col_all]

        sniff_results = {'files_columns': col_files, 'columns_all': col_all, 'columns_common': col_common,
                       'columns_unique': col_unique, 'is_all_equal': columns_all_equal(dfl_all_col),
                       'df_columns_present': df_col_present, 'df_columns_order': df_col_idx}
        self.sniff_results = sniff_results

        return sniff_results

    def get_sniff_results(self):
        if not self.sniff_results:
            self.sniff_columns()
        return self.sniff_results

    def _sniff_available(self):
        if not self.sniff_results:
            self.sniff_columns()

    def is_all_equal(self):
        """
        Checks if all columns are equal in all files

        Returns:
             bool: all columns are equal in all files?
        """
        self._sniff_available()
        return self.sniff_results['is_all_equal']

    def is_column_present(self):
        """
        Shows which columns are present in which files

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self._sniff_available()
        return self.sniff_results['df_columns_present']

    def is_column_present_unique(self):
        """
        Shows unique columns by file

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self._sniff_available()
        return self.is_column_present()[self.sniff_results['columns_unique']]

    def columns_unique(self):
        """
        Shows unique columns by file

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self.columns_unique()

    def is_column_present_common(self):
        """
        Shows common columns by file        

        Returns:
             dataframe: boolean values for column presence in each file
        """
        self._sniff_available()
        return self.is_column_present()[self.sniff_results['columns_common']]

    def columns_common(self):
        """
        Shows common columns by file        

        Returns:
             dataframe: boolean values for column presence in each file
        """
        return self.is_column_present_common()

    def columns(self):
        """
        Shows columns by file        

        Returns:
             dict: filename, columns
        """
        self._sniff_available()
        return self.sniff_results['files_columns']

    def head(self):
        """
        Shows preview rows for each file

        Returns:
             dict: filename, dataframe
        """
        self._sniff_available()
        return dict(zip(self.fname_list,self.dfl_all))

    def _columns_reindex_prep(self):

        self._sniff_available()
        self._columns_select_dict = {} # select columns by filename
        self._columns_rename_dict = {} # rename columns by filename

        for fname in self.fname_list:
            if self.columns_rename:
                columns_rename = self.columns_rename.copy()
                # check no naming conflicts
                columns_select2 = [columns_rename[k] if k in columns_rename.keys() else k for k in self.sniff_results['files_columns'][fname]]
                df_rename_count = collections.Counter(columns_select2)
                if df_rename_count and max(df_rename_count.values()) > 1:  # would the rename create naming conflict?
                    warnings.warn('Renaming conflict: {}'.format([(k, v) for k, v in df_rename_count.items() if v > 1]),
                                  UserWarning)
                    while df_rename_count and max(df_rename_count.values()) > 1:
                        # remove key value pair causing conflict
                        conflicting_keys = [i for i, j in df_rename_count.items() if j > 1]
                        columns_rename = {k: v for k, v in columns_rename.items() if k in conflicting_keys}
                        columns_select2 = [columns_rename[k] if k in columns_rename.keys() else k for k in
                                           self.sniff_results['files_columns'][fname]]
                        df_rename_count = collections.Counter(columns_select2)

                # store rename by file. keep only renames for columns actually present in file
                self._columns_rename_dict[fname] = dict((k,v) for k,v in columns_rename.items() if k in k in self.sniff_results['files_columns'][fname])

        if self.columns_select:
            columns_select2 = self.columns_select.copy()
        else:
            if self.columns_select_common:
                columns_select2 = self.sniff_results['columns_common'].copy()
            else:
                columns_select2 = self.sniff_results['columns_all'].copy()

        if self.columns_rename:
            columns_select2 = list(dict.fromkeys([columns_rename[k] if k in columns_rename.keys() else k for k in columns_select2]))  # set of columns after rename
        # store select by file
        self._columns_reindex = columns_select2

    def _columns_reindex_available(self):
        if not self._columns_rename_dict or not self._columns_reindex:
            self._columns_reindex_prep()

    def preview_rename(self):
        """
        Shows which columns will be renamed in processing

        Returns:
            dataframe: columns to be renamed from which file
        """
        self._columns_reindex_available()
        df = pd.DataFrame(self._columns_rename_dict).T
        return df

    def preview_select(self):
        """
        Shows which columns will be selected in processing

        Returns:
            list: columns to be selected from all files
        """
        self._columns_reindex_available()
        return self._columns_reindex

    def combine_preview(self):
        """
        Preview of what the combined data will look like

        Returns:
            dataframe: combined dataframe
        """
        read_csv_params = copy.deepcopy(self.read_csv_params)
        read_csv_params['nrows'] = self.nrows_preview

        df = [[dfc for dfc in self._read_csv_yield(fname, read_csv_params)] for fname in self.fname_list]
        df = _dfconact(df)
        self.df_combine_preview = df.copy()
        return df

    def _combine_preview_available(self):
        if self.df_combine_preview is None:
            self.combine_preview()

    def to_pandas(self):
        """
        Combine all files to a pandas dataframe

        Returns:
            dataframe: combined data
        """
        df = [[dfc for dfc in self._read_csv_yield(fname, self.read_csv_params)] for fname in self.fname_list]
        df = _dfconact(df)
        return df

    def _get_filepath_out(self, fname, output_dir, output_prefix, ext):
        # filename
        fname_out = ntpath.basename(fname)
        fname_out = os.path.splitext(fname_out)[0]
        fname_out = output_prefix + fname_out + ext

        # path
        output_dir = output_dir if output_dir else os.path.dirname(fname)
        fpath_out = os.path.join(output_dir, fname_out)
        assert _direxists(fpath_out, self.logger)
        return fpath_out

    def _to_csv_prep(self, write_params):
        if 'index' not in write_params:
            write_params['index'] = False
        write_params.pop('header', None) # library handles

        self._combine_preview_available()

        return write_params

    def to_csv_head(self, output_dir=None, write_params={}):
        """
        Save `nrows_preview` header rows as individual files

        Args:
            output_dir (str): directory to save files in. If not given save in the same directory as the original file
            write_params (dict): additional params to pass to `pandas.to_csv()`

        Returns:
            list: list of filenames of processed files
        """

        write_params = self._to_csv_prep(write_params)

        fnamesout = []
        for fname, dfg in dict(zip(self.fname_list,self.dfl_all)).items():
            filename = f'{fname}-head.csv'
            filename = filename if output_dir is None else str(pathlib.Path(output_dir)/filename)
            dfg.to_csv(filename, **write_params)
            fnamesout.append(filename)

        return fnamesout

    def to_csv_align(self, output_dir=None, output_prefix='d6tstack-', write_params={}):
        """
        Create cleaned versions of original files. Automatically runs out of core, using `self.chunksize`.

        Args:
            output_dir (str): directory to save files in. If not given save in the same directory as the original file
            output_prefix (str): prepend with prefix to distinguish from original files
            write_params (dict): additional params to pass to `pandas.to_csv()`

        Returns:
            list: list of filenames of processed files
        """
        # stream all chunks to multiple files

        write_params = self._to_csv_prep(write_params)

        fnamesout = []
        for fname in self.fname_list:
            filename = self._get_filepath_out(fname, output_dir, output_prefix, '.csv')
            if self.logger:
                self.logger.send_log('writing '+filename , 'ok')
            fhandle = open(filename, 'w')
            self.df_combine_preview[:0].to_csv(fhandle, **write_params)
            for dfc in self._read_csv_yield(fname, self.read_csv_params):
                dfc.to_csv(fhandle, header=False, **write_params)
            fhandle.close()
            fnamesout.append(filename)

        return fnamesout

    def to_csv_combine(self, filename, write_params={}):
        """
        Combines all files to a single csv file. Automatically runs out of core, using `self.chunksize`.

        Args:
            filename (str): file names
            write_params (dict): additional params to pass to `pandas.to_csv()`

        Returns:
            str: filename for combined data
        """
        # stream all chunks from all files to a single file
        write_params = self._to_csv_prep(write_params)

        assert _direxists(filename, self.logger)
        fhandle = open(filename, 'w')
        self.df_combine_preview[:0].to_csv(fhandle, **write_params)
        for fname in self.fname_list:
            for dfc in self._read_csv_yield(fname, self.read_csv_params):
                dfc.to_csv(fhandle, header=False, **write_params)
        fhandle.close()
        return filename

    def to_parquet_align(self, output_dir=None, output_prefix='d6tstack-', write_params={}):
        """
        Same as `to_csv_align` but outputs parquet files

        """
        # write_params for pyarrow.parquet.write_table

        # stream all chunks to multiple files
        self._combine_preview_available()

        import pyarrow as pa
        import pyarrow.parquet as pq

        fnamesout = []
        pqschema = pa.Table.from_pandas(self.df_combine_preview).schema
        for fname in self.fname_list:
            filename = self._get_filepath_out(fname, output_dir, output_prefix, '.pq')
            if self.logger:
                self.logger.send_log('writing '+filename , 'ok')
            pqwriter = pq.ParquetWriter(filename, pqschema)
            for dfc in self._read_csv_yield(fname, self.read_csv_params):
                pqwriter.write_table(pa.Table.from_pandas(dfc.astype(self.df_combine_preview.dtypes), schema=pqschema),**write_params)
            pqwriter.close()
            fnamesout.append(filename)

        return fnamesout

    def to_parquet_combine(self, filename, write_params={}):
        """
        Same as `to_csv_combine` but outputs parquet files

        """
        # stream all chunks from all files to a single file
        self._combine_preview_available()

        assert _direxists(filename, self.logger)
        import pyarrow as pa
        import pyarrow.parquet as pq

        # todo: fix mixed data type writing. at least give a warning
        pqwriter = pq.ParquetWriter(filename, pa.Table.from_pandas(self.df_combine_preview).schema)
        for fname in self.fname_list:
            for dfc in self._read_csv_yield(fname, self.read_csv_params):
                pqwriter.write_table(pa.Table.from_pandas(dfc.astype(self.df_combine_preview.dtypes)),**write_params)
        pqwriter.close()
        return filename

    def to_sql_combine(self, uri, tablename, if_exists='fail', write_params=None, return_create_sql=False):
        """
        Load all files into a sql table using sqlalchemy. Generic but slower than the optmized functions

        Args:
            uri (str): sqlalchemy database uri
            tablename (str): table to store data in
            if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’. See `pandas.to_sql()` for details
            write_params (dict): additional params to pass to `pandas.to_sql()`
            return_create_sql (dict): show create sql statement for combined file schema. Doesn't run data load

        Returns:
            bool: True if loader finished

        """
        if not write_params:
            write_params = {}
        if 'if_exists' not in write_params:
            write_params['if_exists'] = if_exists
        if 'index' not in write_params:
            write_params['index'] = False
        self._combine_preview_available()

        if 'mysql' in uri and not 'mysql+pymysql' in uri:
            raise ValueError('need to use pymysql for mysql (pip install pymysql)')

        import sqlalchemy

        sql_engine = sqlalchemy.create_engine(uri)

        # create table
        dfhead = self.df_combine_preview.astype(self.df_combine_preview.dtypes)[:0]

        if return_create_sql:
            return pd.io.sql.get_schema(dfhead, tablename).replace('"',"`")

        dfhead.to_sql(tablename, sql_engine, **write_params)

        # append data
        write_params['if_exists'] = 'append'
        for fname in self.fname_list:
            for dfc in self._read_csv_yield(fname, self.read_csv_params):
                dfc.astype(self.df_combine_preview.dtypes).to_sql(tablename, sql_engine, **write_params)

        return True

    def to_psql_combine(self, uri, table_name, if_exists='fail', sep=','):
        """
        Load all files into a sql table using native postgres COPY FROM. Chunks data load to reduce memory consumption

        Args:
            uri (str): postgres psycopg2 sqlalchemy database uri
            table_name (str): table to store data in
            if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’. See `pandas.to_sql()` for details
            sep (str): separator for temp file, eg ',' or '\t'

        Returns:
            bool: True if loader finished

        """

        if not 'psycopg2' in uri:
            raise ValueError('need to use psycopg2 uri')

        self._combine_preview_available()

        import sqlalchemy
        import io

        sql_engine = sqlalchemy.create_engine(uri)
        sql_cnxn = sql_engine.raw_connection()
        cursor = sql_cnxn.cursor()

        self.df_combine_preview[:0].to_sql(table_name, sql_engine, if_exists=if_exists, index=False)

        for fname in self.fname_list:
            for dfc in self._read_csv_yield(fname, self.read_csv_params):
                fbuf = io.StringIO()
                dfc.astype(self.df_combine_preview.dtypes).to_csv(fbuf, index=False, header=False, sep=sep)
                fbuf.seek(0)
                cursor.copy_from(fbuf, table_name, sep=sep, null='')
        sql_cnxn.commit()
        cursor.close()

        return True

    def to_mysql_combine(self, uri, table_name, if_exists='fail', tmpfile='mysql.csv', sep=','):
        """
        Load all files into a sql table using native postgres LOAD DATA LOCAL INFILE. Chunks data load to reduce memory consumption

        Args:
            uri (str): mysql mysqlconnector sqlalchemy database uri
            table_name (str): table to store data in
            if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’. See `pandas.to_sql()` for details
            tmpfile (str): filename for temporary file to load from
            sep (str): separator for temp file, eg ',' or '\t'

        Returns:
            bool: True if loader finished

        """
        if not 'mysql+mysqlconnector' in uri:
            raise ValueError('need to use mysql+mysqlconnector uri (pip install mysql-connector)')

        self._combine_preview_available()

        import sqlalchemy

        sql_engine = sqlalchemy.create_engine(uri)

        self.df_combine_preview[:0].to_sql(table_name, sql_engine, if_exists=if_exists, index=False)

        if self.logger:
            self.logger.send_log('creating ' + tmpfile, 'ok')
        self.to_csv_combine(tmpfile, write_params={'na_rep':'\\N','sep':sep})
        if self.logger:
            self.logger.send_log('loading ' + tmpfile, 'ok')
        sql_load = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}' IGNORE 1 LINES;".format(tmpfile, table_name, sep)
        sql_engine.execute(sql_load)

        os.remove(tmpfile)

        return True

    def to_mssql_combine(self, uri, table_name, schema_name=None, if_exists='fail', tmpfile='mysql.csv'):
        """
        Load all files into a sql table using native postgres LOAD DATA LOCAL INFILE. Chunks data load to reduce memory consumption

        Args:
            uri (str): mysql mysqlconnector sqlalchemy database uri
            table_name (str): table to store data in
            schema_name (str): name of schema to write to
            if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’. See `pandas.to_sql()` for details
            tmpfile (str): filename for temporary file to load from

        Returns:
            bool: True if loader finished

        """
        if not 'mssql+pymssql' in uri:
            raise ValueError('need to use mssql+pymssql uri (conda install -c prometeia pymssql)')

        self._combine_preview_available()

        import sqlalchemy

        sql_engine = sqlalchemy.create_engine(uri)

        self.df_combine_preview[:0].to_sql(table_name, sql_engine, schema=schema_name, if_exists=if_exists, index=False)

        if self.logger:
            self.logger.send_log('creating ' + tmpfile, 'ok')
        self.to_csv_combine(tmpfile, write_params={'na_rep':'\\N'})
        if self.logger:
            self.logger.send_log('loading ' + tmpfile, 'ok')
        if schema_name is not None:
            table_name = '{}.{}'.format(schema_name,table_name)
        sql_load = "BULK INSERT {} FROM '{}';".format()(table_name, tmpfile)
        sql_engine.execute(sql_load)

        os.remove(tmpfile)

        return True

# todo: ever need to rerun _available fct instead of using cache?
