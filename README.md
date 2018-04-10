# Databolt python library

## Accelerate data engineering

DataBolt is a collection of products and libraries to reduce the time it takes to get your data ready for evaluation and analysis.

## File Combiner

Automatically combine multiple files into one by stacking them together. Works for .xls, .csv, .txt.

Vendors often send large datasets in multiple files. Often there are missing and misaligned columns between files that have to be manually cleaned. With DataBolt File Combiner you can easily merge them together into one dataframe.

![](https://www.databolt.tech/images/combiner-landing-git.png)

Features include

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
