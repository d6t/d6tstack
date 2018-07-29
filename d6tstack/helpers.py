#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Module with several helper functions

"""

import os
import collections
import re

def file_extensions_get(fname_list):
    """Returns file extensions in list

    Args:
        fname_list (list): file names, eg ['a.csv','b.csv']

    Returns:
        list: file extensions for each file name in input list, eg ['.csv','.csv']
    """
    return [os.path.splitext(fname)[-1] for fname in fname_list]


def file_extensions_all_equal(ext_list):
    """Checks that all file extensions are equal. 

    Args:
        ext_list (list): file extensions, eg ['.csv','.csv']

    Returns:
        bool: all extensions are equal to first extension in list?
    """
    return len(set(ext_list))==1


def file_extensions_contains_xls(ext_list):
    # Assumes all file extensions are equal! Only checks first file
    return ext_list[0] == '.xls'

def file_extensions_contains_xlsx(ext_list):
    # Assumes all file extensions are equal! Only checks first file
    return ext_list[0] == '.xlsx'

def file_extensions_contains_csv(ext_list):
    # Assumes all file extensions are equal! Only checks first file
    return (ext_list[0] == '.csv' or ext_list[0] == '.txt')

def file_extensions_valid(ext_list):
    """Checks if file list contains only valid files

    Notes:
        Assumes all file extensions are equal! Only checks first file

    Args:
        ext_list (list): file extensions, eg ['.csv','.csv']

    Returns:
        bool: first element in list is one of ['.csv','.txt','.xls','.xlsx']?
    """
    ext_list_valid = ['.csv','.txt','.xls','.xlsx']
    return ext_list[0] in ext_list_valid


def columns_all_equal(col_list):
    """Checks that all lists in col_list are equal. 

    Args:
        col_list (list): columns, eg [['a','b'],['a','b','c']]

    Returns:
        bool: all lists in list are equal?
    """
    return all([l==col_list[0] for l in col_list])


def list_common(_list, sort=True):
    l = list(set.intersection(*[set(l) for l in _list]))
    if sort:
        return sorted(l)
    else:
        return l


def list_unique(_list, sort=True):
    l = list(set.union(*[set(l) for l in _list]))
    if sort:
        return sorted(l)
    else:
        return l


def list_tofront(_list,val):
    return _list.insert(0, _list.pop(_list.index(val)))


def cols_filename_tofront(_list):
    return list_tofront(_list,'filename')


def df_filename_tofront(dfg):
    cfg_col = dfg.columns.tolist()
    return dfg[cols_filename_tofront(cfg_col)]


def check_valid_xls(fname_list):
    ext_list = file_extensions_get(fname_list)

    if not file_extensions_all_equal(ext_list):
        raise IOError('All file types and extensions have to be equal')

    if not(file_extensions_contains_xls(ext_list) or file_extensions_contains_xlsx(ext_list)):
        raise IOError('Only .xls, .xlsx files can be processed')

    return True


def compare_pandas_versions(version1, version2):
    def cmp(a, b):
        return (a > b) - (a < b)

    def normalize(v):
        return [int(x) for x in re.sub(r'(\.0+)*$','', v).split(".")]

    return cmp(normalize(version1), normalize(version2))
