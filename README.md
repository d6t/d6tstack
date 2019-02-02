# Databolt File Ingest

Quickly ingest raw files. Works for XLS, CSV, TXT which can be exported to CSV, Parquet, SQL and Pandas. `d6tstack` solves many performance and schema problems typically encountered when ingesting raw files. 

![](https://www.databolt.tech/images/combiner-landing-git.png)

### Features include

* Fast pd.to_sql() for postgres and mysql
* Quickly check columns for consistency across files
* Fix added/missing columns
* Fix renamed columns
* Check Excel tabs for consistency across files
* Excel to CSV converter (incl multi-sheet support)
* Out of core functionality to process large files
* Export to CSV, parquet, SQL, pandas dataframe

### Sample Use

```

import d6tstack

# fast CSV to SQL import - see SQL examples notebook
d6tstack.utils.pd_to_psql(df, 'postgresql+psycopg2://usr:pwd@localhost/db', 'tablename')
d6tstack.utils.pd_to_mysql(df, 'mysql+mysqlconnector://usr:pwd@localhost/db', 'tablename')
d6tstack.utils.pd_to_mssql(df, 'mssql+pymssql://usr:pwd@localhost/db', 'tablename') # experimental

# ingest mutiple CSVs which may have data schema changes - see CSV examples notebook

import glob
>>> c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/*.csv'))

# quick check if all files have consistent columns
>>> c.is_all_equal()
False

# show which files have missing columns
>>> c.is_col_present()
   filename  cost  date profit profit2 sales
0  feb.csv  True  True   True   False  True
2  mar.csv  True  True   True    True  True

>>> c.combine_preview() # keep all columns
   filename  cost        date profit profit2 sales
0   jan.csv  -80  2011-01-01     20     NaN   100
0   mar.csv  -100  2011-03-01    200     400   300

>>> d6tstack.combine_csv.CombinerCSV(glob.glob('*.csv'), columns_select_common=True).combine_preview() # keep common columns
   filename  cost        date profit sales
0   jan.csv  -80  2011-01-01     20   100
0   mar.csv  -100  2011-03-01    200   300

>>> d6tstack.combine_csv.CombinerCSV(glob.glob('*.csv'), columns_rename={'sales':'revenue'}).combine_preview()
   filename  cost        date profit profit2 revenue
0   jan.csv  -80  2011-01-01     20     NaN   100
0   mar.csv  -100  2011-03-01    200     400   300

# to come: check if columns match database
>>> c.is_columns_match_db('postgresql+psycopg2://usr:pwd@localhost/db', 'tablename')

# export to csv, parquet, sql. Out of core with optimized fast imports for postgres and mysql
>>> c.to_pandas()
>>> c.to_csv_align(output_dir='process/')
>>> c.to_parquet_align(output_dir='process/')
>>> c.to_sql_combine('postgresql+psycopg2://usr:pwd@localhost/db', 'tablename')
>>> c.to_psql_combine('postgresql+psycopg2://usr:pwd@localhost/db', 'tablename') # fast, using COPY FROM
>>> c.to_mysql_combine('mysql+mysqlconnector://usr:pwd@localhost/db', 'tablename') # fast, using LOAD DATA LOCAL INFILE

# read Excel files - see Excel examples notebook for more details
import d6tstack.convert_xls

d6tstack.convert_xls.read_excel_advanced('test.xls',
    sheet_name='Sheet1', header_xls_range="B2:E2")

d6tstack.convert_xls.XLStoCSVMultiSheet('test.xls').convert_all(header_xls_range="B2:E2")

d6tstack.convert_xls.XLStoCSVMultiFile(glob.glob('*.xls'), 
    cfg_xls_sheets_sel_mode='name_global',cfg_xls_sheets_sel='Sheet1')
    .convert_all(header_xls_range="B2:E2")

```


## Installation

We recommend using the latest version from github `pip install git+https://github.com/d6t/d6tstack.git`. 

If you cannot install from github, use the latest published version `pip install d6tstack`. For Excel and parquet support, install `d6tstack[xls]` and `d6tstack[parquet]`. Certain database specific function require packages which you will be prompted for as you use them.


## Documentation

*  [CSV examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-csv.ipynb) - Quickly load any type of CSV files
*  [Excel examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-excel.ipynb) - Quickly extract from Excel to CSV 
*  [Dask Examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-dask.ipynb) - How to use d6tstack to solve Dask input file problems
*  [Pyspark Examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-pyspark.ipynb) - How to use d6tstack to solve pyspark input file problems
*  [SQL examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-sql.ipynb) - Fast loading of CSV to SQL with pandas preprocessing
*  [Function reference docs](http://d6tstack.readthedocs.io/en/latest/py-modindex.html) - Detailed documentation for modules, classes, functions
*  [www.databolt.tech](https://www.databolt.tech/index-combine.html) - Web app if you don't want to code

## Faster Data Engineering

Check out other d6t libraries to solve common data engineering problems, including  
* data ingest, quickly ingest raw data
* fuzzy joins, quickly join data
* data pipes, quickly share and distribute data

https://github.com/d6t/d6t-python

And we encourage you to join the Databolt blog to get updates and tips+tricks http://blog.databolt.tech

## Collecting Errors Messages and Usage statistics

To help us make this library better, it collects anonymous error messages and usage statistics. It works similar to how websites collect data. See [d6tcollect](https://github.com/d6t/d6tcollect) for details including how to disable collection.

It might not catch all errors so if you run into any problems, please raise an issue on github.