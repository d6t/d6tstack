# Databolt File Stack

Automatically combine multiple files into one by stacking them together. Works for XLS, CSV, TXT which can be exported to CSV, Parquet, SQL and Pandas.

Vendors often send large datasets in multiple files. Often there are missing and misaligned columns between files that have to be manually cleaned. With DataBolt File Combiner you can easily stack them together into one dataframe.

![](https://www.databolt.tech/images/combiner-landing-git.png)

### Features include

* Quickly check columns for consistency across files
* Fix added/missing columns
* Fix renamed columns
* Check Excel tabs for consistency across files
* Excel to CSV converter (incl multi-sheet support)
* Out of core functionality to process large files
* Export to CSV, parquet, SQL, pandas dataframe

### Sample Use

```

import glob
import d6tstack
>>> c = d6tstack.combine_csv.CombinerCSV(glob.glob('*.csv'))

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

>>> d6tstack.combine_csv.CombinerCSV(glob.glob('*.csv'), columns_select_common=True) # keep common columns
   filename  cost        date profit sales
0   jan.csv  -80  2011-01-01     20   100
0   mar.csv  -100  2011-03-01    200   300

>>> d6tstack.combine_csv.CombinerCSV(glob.glob('*.csv'), columns_rename={'sales':'revenue'})
   filename  cost        date profit profit2 revenue
0   jan.csv  -80  2011-01-01     20     NaN   100
0   mar.csv  -100  2011-03-01    200     400   300

# export to csv, parquet, sql. Out of core with optimized fast imports for postgres and mysql
>>> c.to_csv_align(output_dir='process/')
>>> c.to_parquet_align(output_dir='process/')
>>> c.to_sql_combine('postgresql+psycopg2://usr:pwd@localhost/db', 'tablename')
>>> c.to_psql_combine('postgresql+psycopg2://usr:pwd@localhost/db', 'tablename') # fast, using COPY FROM
>>> c.to_mysql_combine('mysql+mysqlconnector://usr:pwd@localhost/db', 'tablename') # fast, using LOAD DATA LOCAL INFILE

```


## Installation

We recommend using the latest version from github `pip install git+https://github.com/d6t/d6tstack.git`

If you cannot install from github, use the latest published version `pip install d6tstack`. For SQL and parquet support, additionally install `d6tstack[sql]` and `d6tstack[parquet]`.


## Documentation

*  [CSV examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-csv.ipynb) - Quickly load any type of CSV files
*  [Excel examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-excel.ipynb) - Quickly extract from Excel to CSV 
*  [Dask Examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-dask.ipynb) - How to use d6tstack to solve Dask input file problems
*  [Pyspark Examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-pyspark.ipynb) - How to use d6tstack to solve pyspark input file problems
*  [SQL examples notebook](https://github.com/d6t/d6tstack/blob/master/examples-sql.ipynb) - Fast loading of CSV to SQL with pandas preprocessing
*  [Official docs](http://d6tstack.readthedocs.io/en/latest/py-modindex.html) - Detailed documentation for modules, classes, functions
*  [www.databolt.tech](https://www.databolt.tech/index-combine.html) - Web app if you don't want to code
