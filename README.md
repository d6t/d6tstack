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

>>> c.col_preview['is_all_equal']
False

>>> c.col_preview['df_columns_present']
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
