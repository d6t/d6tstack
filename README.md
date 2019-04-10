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

## Installation

Latest published version `pip install d6tstack`. Additoinal requirements:
* `d6tstack[psql]`: for pandas to postgres
* `d6tstack[mysql]`: for pandas to mysql
* `d6tstack[xls]`: for excel support
* `d6tstack[parquet]`: for ingest csv to parquet

Latest dev version from github `pip install git+https://github.com/d6t/d6tstack.git`  

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


## Documentation

*  [SQL examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-sql.ipynb) - Fast loading of CSV to SQL with pandas preprocessing
*  [CSV examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-csv.ipynb) - Quickly load any type of CSV files
*  [Excel examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-excel.ipynb) - Quickly extract from Excel to CSV 
*  [Dask Examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-dask.ipynb) - How to use d6tstack to solve Dask input file problems
*  [Pyspark Examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-pyspark.ipynb) - How to use d6tstack to solve pyspark input file problems
*  [Function reference docs](http://d6tstack.readthedocs.io/en/latest/py-modindex.html) - Detailed documentation for modules, classes, functions

## Faster Data Engineering

Check out other d6t libraries to solve common data engineering problems, including  
* data worfklows,build highly effective data science workflows
* fuzzy joins, quickly join data
* data pipes, quickly share and distribute data

https://github.com/d6t/d6t-python

And we encourage you to join the Databolt blog to get updates and tips+tricks http://blog.databolt.tech

## Collecting Errors Messages and Usage statistics

We have put a lot of effort into making this library useful to you. To help us make this library even better, it collects ANONYMOUS error messages and usage statistics. See [d6tcollect](https://github.com/d6t/d6tcollect) for details including how to disable collection. Collection is asynchronous and doesn't impact your code in any way.

It may not catch all errors so if you run into any problems or have any questions, please raise an issue on github.