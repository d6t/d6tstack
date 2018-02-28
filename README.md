# Databolt python library

## Accelerate data engineering

DataBolt provides products to reduce the time it takes to get your data ready for evaluation and analysis.

[www.databolt.tech](https://www.databolt.tech/) - Landing page & web app

## File Combiner

Automatically combine multiple files into one by stacking them together. Works for .xls, .csv, .txt.

![](https://www.databolt.tech/images/combiner-landing-small.png)

Example features include

* Scan headers of all files to check column names => useful QA tool before using dask or pyspark
* Select and rename columns in multiple files
* CSV Settings sniffer
* Excel tab sniffer
* Excel to CSV converter (incl multi-sheet support)
* Out of core functionality
* Export to CSV or pandas dataframe

## Smart Join

Easily join different datasets without writing custom code. Does fuzzy and time-series aware joins.
__coming soon__

## Installation

Install `pip install git+https://github.com/d6t/d6t-lib.git`

Update `pip install --upgrade git+https://github.com/d6t/d6t-lib.git`

## Documentation

*  [Official docs](http://d6t.readthedocs.io/en/latest/d6t.stack.html) - Detailed documentation for modules, classes, functions
*  [Examples notebook](https://github.com/d6t/d6t-lib/blob/master/examples.ipynb) - Demonstrates usage
*  [www.databolt.tech](https://www.databolt.tech/) - Landing page & web app
