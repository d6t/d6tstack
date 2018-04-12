# Databolt File Stack

Automatically combine multiple files into one by stacking them together. Works for .xls, .csv, .txt.

Vendors often send large datasets in multiple files. Often there are missing and misaligned columns between files that have to be manually cleaned. With DataBolt File Combiner you can easily stack them together into one dataframe.

![](https://www.databolt.tech/images/combiner-landing-git.png)

### Sample Use

```python

import glob
from d6tstack.stack import combine_csv
>>> c = combine_csv.CombinerCSV(glob.glob('*.csv'))

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


## Installation

Install `pip install git+https://github.com/d6t/d6t-lib.git`

Update `pip install --upgrade git+https://github.com/d6t/d6t-lib.git`


## Documentation

*  [Combiner Examples notebook](https://github.com/d6t/d6t-lib/blob/master/examples-combiner.ipynb) - Demonstrates combiner usage
*  [Official docs](http://d6tstack.readthedocs.io/en/latest/index.html) - Detailed documentation for modules, classes, functions
*  [www.databolt.tech](https://www.databolt.tech/index-combine.html) - Web app if you don't want to code
