# Databolt python library

## Accelerate data engineering

DataBolt is a collection of products and libraries to reduce the time it takes to get your data ready for evaluation and analysis.

## File Combiner

Automatically combine multiple files into one by stacking them together. Works for .xls, .csv, .txt.

Vendors often send large datasets in multiple files. Often there are missing and misaligned columns between files that have to be manually cleaned. With DataBolt File Combiner you can easily merge them together into one dataframe.

![](https://www.databolt.tech/images/combiner-landing-git.png)

### Sample Use

```python

import glob
import d6t.stack.combine_csv as d6tc
>>> c = d6tc.CombinerCSV(glob.glob('*.csv'))

# quick check if all files have consistent columns
>>> c.is_all_equal()
False

# show which files are missing columns
>>> c.is_col_present()
   filename  cost  date profit profit2 sales
0  feb.csv  True  True   True   False  True
1  jan.csv  True  True   True   False  True
2  mar.csv  True  True   True    True  True

>>> c.combine_preview() # keep all columns
   filename  cost        date profit profit2 sales
0   jan.csv  -80  2011-01-01     20     NaN   100
0   feb.csv  -90  2011-02-01    110     NaN   200
0   mar.csv  -100  2011-03-01    200     400   300

>>> c.combine_preview(is_col_common=True) # keep common columns
   filename  cost        date profit sales
0   jan.csv  -80  2011-01-01     20   100
0   feb.csv  -90  2011-02-01    110   200
0   mar.csv  -100  2011-03-01    200   300

```



### Features include

* Scan headers of all files to check column names => useful QA tool before using dask or pyspark
* Select and rename columns in multiple files
* CSV Settings sniffer
* Excel tab sniffer
* Excel to CSV converter (incl multi-sheet support)
* Out of core functionality
* Export to CSV or pandas dataframe

[Combiner Examples notebook](https://github.com/d6t/d6t-lib/blob/master/examples-combiner.ipynb)

## Smart Join

Easily join different datasets without writing custom code. Does fuzzy and time-series aware joins.

![](https://www.databolt.tech/images/joiner-landing-git.png)

### Sample Use

```python

import pandas as pd

import join.smart_join

df1=pd.read_csv('data/case_factors/securities.csv',parse_dates=['date'])
df2=pd.read_csv('data/case_factors/factors.csv',parse_dates=['Date'])

sj = join.smart_join.SmartJoin([df1, df2], [['BARRA_PIT_CUSIP','cusip'],['date','Date']], mode=['top1', 'top1'], how='left', cfg_top1={'BARRA_PIT_CUSIP':{'top_records':5}})

>>> sj.stats_prejoin(do_print=False)

          key left key right  all matched  inner  left  right  outer  unmatched total  unmatched left  unmatched right
0  BARRA_PIT_CUSIP     cusip        False      0   628  12692  13320            13320             628            12692
1             date      Date        False      1     2      2      3                2               1                1
2          __all__   __all__        False      0  1252  22975  24227            24227            1252            22975

>>> df_merge_top1 = sj.run_match_top1('BARRA_PIT_CUSIP')
>>> df_merge_top1['table'].head()
      __top1left__ __top1right__  __top1diff__ __match type__
60731  b'19416210'     194162103             3      top1 left
36934  b'20588710'     205887102             3      top1 left
20183  b'27864210'     278642103             3      top1 left
38268  b'54042410'     540424108             3      top1 left
4732   b'H1467J10'     H1467J104             3      top1 left

```

### Features include
Enhances `pd.merge()` function with:
* Pre- and post-join diagnostics
* Customize join type by join key
* Fuzzy top1 similarity joins for strings and dates
* Multi-frame joins
* Easier UI


[SmartJoin Examples notebook](https://github.com/d6t/d6t-lib/blob/master/examples-smartjoin.ipynb)

## Installation

Install `pip install git+https://github.com/d6t/d6t-lib.git`

Update `pip install --upgrade git+https://github.com/d6t/d6t-lib.git`

## Documentation

*  [Combiner Examples notebook](https://github.com/d6t/d6t-lib/blob/master/examples-combiner.ipynb) - Demonstrates combiner usage
*  [SmartJoin Examples notebook](https://github.com/d6t/d6t-lib/blob/master/examples-smartjoin.ipynb) - Demonstrates SmartJoin usage
*  [Official docs](http://d6t.readthedocs.io/en/latest/d6t.stack.html) - Detailed documentation for modules, classes, functions
*  [www.databolt.tech](https://www.databolt.tech/index-combine.html) - Web app if you don't want to code
