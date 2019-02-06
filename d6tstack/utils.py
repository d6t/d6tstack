import pandas as pd
import warnings

import d6tcollect
d6tcollect.init(__name__)

class PrintLogger(object):
    def send_log(self, msg, status):
        print(msg,status)

    def send(self, data):
        print(data)

import os

@d6tcollect.collect
def pd_readsql_query_from_sqlengine(uri, sql, schema_name=None, connect_args=None):
    """
    Load SQL statement into pandas dataframe using `sql_engine.execute` making execution faster.

    Args:
        uri (str): postgres psycopg2 sqlalchemy database uri
        sql (str): sql query
        schema_name (str): name of schema
        connect_args (dict): dictionary of connection arguments to pass to `sqlalchemy.create_engine`

    Returns:
        df: pandas dataframe

    """

    import sqlalchemy
    if connect_args is not None:
        sql_engine = sqlalchemy.create_engine(uri, connect_args=connect_args)
    elif schema_name is not None:
        if 'psycopg2' in uri:
            sql_engine = sqlalchemy.create_engine(uri, connect_args={'options': '-csearch_path={}'.format(schema_name)})
        else:
            raise NotImplementedError('only `psycopg2` supported with schema_name, pass connect_args for your db engine')
    else:
        sql_engine = sqlalchemy.create_engine(uri)

    sql = sql_engine.execute(sql)
    df = pd.DataFrame(sql.fetchall())

    return df


@d6tcollect.collect
def pd_readsql_table_from_sqlengine(uri, table_name, schema_name=None, connect_args=None):
    """
    Load SQL table into pandas dataframe using `sql_engine.execute` making execution faster. Convenience function that returns full table.

    Args:
        uri (str): postgres psycopg2 sqlalchemy database uri
        table_name (str): table
        schema_name (str): name of schema
        connect_args (dict): dictionary of connection arguments to pass to `sqlalchemy.create_engine`

    Returns:
        df: pandas dataframe

    """

    return pd_readsql_query_from_sqlengine(uri, "SELECT * FROM {};".fromat(table_name), schema_name=schema_name, connect_args=connect_args)


@d6tcollect.collect
def pd_to_psql(df, uri, table_name, schema_name=None, if_exists='fail', sep=','):
    """
    Load pandas dataframe into a sql table using native postgres COPY FROM.

    Args:
        df (dataframe): pandas dataframe
        uri (str): postgres psycopg2 sqlalchemy database uri
        table_name (str): table to store data in
        schema_name (str): name of schema to write to
        if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’. See `pandas.to_sql()` for details
        sep (str): separator for temp file, eg ',' or '\t'

    Returns:
        bool: True if loader finished

    """

    if not 'psycopg2' in uri:
        raise ValueError('need to use psycopg2 uri')

    import sqlalchemy
    import io

    if schema_name is not None:
        sql_engine = sqlalchemy.create_engine(uri, connect_args={'options': '-csearch_path={}'.format(schema_name)})
    else:
        sql_engine = sqlalchemy.create_engine(uri)
    sql_cnxn = sql_engine.raw_connection()
    cursor = sql_cnxn.cursor()

    df[:0].to_sql(table_name, sql_engine, schema=schema_name, if_exists=if_exists, index=False)

    fbuf = io.StringIO()
    df.to_csv(fbuf, index=False, header=False, sep=sep)
    fbuf.seek(0)
    cursor.copy_from(fbuf, table_name, sep=sep, null='')
    sql_cnxn.commit()
    cursor.close()

    return True


@d6tcollect.collect
def pd_to_mysql(df, uri, table_name, if_exists='fail', tmpfile='mysql.csv', sep=','):
    """
    Load dataframe into a sql table using native postgres LOAD DATA LOCAL INFILE.

    Args:
        df (dataframe): pandas dataframe
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

    import sqlalchemy

    sql_engine = sqlalchemy.create_engine(uri)

    df[:0].to_sql(table_name, sql_engine, if_exists=if_exists, index=False)

    logger = PrintLogger()
    logger.send_log('creating ' + tmpfile, 'ok')
    df.to_csv(tmpfile, na_rep='\\N', index=False, sep=sep)
    logger.send_log('loading ' + tmpfile, 'ok')
    sql_load = "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY '{}' IGNORE 1 LINES;".format(tmpfile, table_name, sep)
    sql_engine.execute(sql_load)

    os.remove(tmpfile)

    return True

@d6tcollect.collect
def pd_to_mssql(df, uri, table_name, schema_name=None, if_exists='fail', tmpfile='mysql.csv'):
    """
    Load dataframe into a sql table using native postgres LOAD DATA LOCAL INFILE.

    Args:
        df (dataframe): pandas dataframe
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

    warnings.warn('`.pd_to_mssql()` is experimental, if any problems please raise an issue on https://github.com/d6t/d6tstack/issues or make a pull request')
    import sqlalchemy

    sql_engine = sqlalchemy.create_engine(uri)

    df[:0].to_sql(table_name, sql_engine, if_exists=if_exists, index=False)

    logger = PrintLogger()
    logger.send_log('creating ' + tmpfile, 'ok')
    df.to_csv(tmpfile, na_rep='\\N', index=False)
    logger.send_log('loading ' + tmpfile, 'ok')
    if schema_name is not None:
        table_name = '{}.{}'.format(schema_name,table_name)
    sql_load = "BULK INSERT {} FROM '{}';".format(table_name, tmpfile)
    sql_engine.execute(sql_load)

    os.remove(tmpfile)

    return True

