#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Module contains logic to combine csv, txt, xls and xlsx files.

todo:
combine_files_csv cfg_col instead of mode. reindex vs all/common
self.read_csv_all(cfg_col_sel=cfg_col_sel) => don't need to reindex if all cols present

"""
import os
import ntpath

import numpy as np
import openpyxl
import xlrd

from .helpers import *
from .helpers_ui import *
from .combine_csv import *
from .convert_xls import *
from .sniffer import *

#******************************************************************
# coordinator
#******************************************************************
def combine_files(fname_list, fname_out_folder, log_pusher, fname_out_base='combined', cfg_settings = None, cfg_return_df=False):
    
    """
    
    Key function to combine files. Unless otherwise specified, takes list of input files, combines them, saves output file

    Note:
        Unless it returns ``status:'complete'``, it will need to take the output provided, ask for user input and be run again with modified settings based on user input

    Raises:
        AssertionError: if any of the various checks fail
        ValueError: if cfg_settings['xls_sheets_sel_mode'] invalid

    Args:
        fname_list (list): excel file names, eg ['a.xls','b.xls']
        fname_out_folder (string): where the combined file should be saved 
        fname_out_base (string): base filename WITHOUT extension 
        log_pusher (object): logger object that sends pusher logs 

            Note:

                File name is fixed at ``fname_out_folder+fname_out_base+.[csv|txt|xls|xlsx]`` where file extension is automatically determined based on input file extension.
        
        cfg_settings (dict): file handling settings based on prior run or user input. Keys are used as follows:

            * ``xls_sheets_sel_mode``: 
                * ``name``: selection by sheet name 
                * ``idx``: selection by sheet index
            * ``xls_sheets_sel``: values of selected sheets. 
                * for ``xls_sheets_sel_mode=='name': {'a.xls':'Sheet1','b.xls':'Sheet1'}``
                * for ``xls_sheets_sel_mode=='idx': {'a.xls':0,'b.xls':1}``

    Returns:
        dict: Returns a dictionary with contents differing by case
            **Case Success** Files were successfully read, combined and saved ::
                
                {'status:':'complete', 'fname':'test/output/combined.csv'} 

            **Case Input required**: excel files have multiple sheets and user needs to select which ones to use. See ``xls_sheets_sel_mode`` and ``xls_sheets_sel`` for required settings to rerun::
                
                {  
                   'status':'need_xls_sheets',
                   'sheets':{  
                      'test-data/test-data-input-xls-mult-jan.xls':{  
                         'sheets_names':[  
                            'Sheet1',
                            'Sheet2'
                         ],
                         'sheets_count':2,
                         'sheets_idx':[  
                            0,
                            1
                         ]
                      },
                      'test-data/test-data-input-xls-mult-feb.xls':{  
                         'sheets_names':[  
                            'Sheet1',
                            'Sheet2'
                         ],
                         'sheets_count':2,
                         'sheets_idx':[  
                            0,
                            1
                         ]
                      },
                      'test-data/test-data-input-xls-mult-mar.xls':{  
                         'sheets_names':[  
                            'Sheet1',
                            'Sheet2'
                         ],
                         'sheets_count':2,
                         'sheets_idx':[  
                            0,
                            1
                         ]
                      }
                   }
                }                 

    """

    if cfg_settings is None:
        cfg_settings = {}

    ext_list = file_extensions_get(fname_list)
    log_pusher.send_log('process files extensions:'+str(ext_list),'ok')
    if not file_extensions_all_equal(ext_list):
        return {'status':'error','msg-error':'all files need to have the same extension','settings':cfg_settings}
    if not file_extensions_valid(ext_list):
        return {'status':'error','msg-error':'invalid file extension, can only  process .xls,.xlsx,.csv,.txt','settings':cfg_settings}

    #******************************************************************
    # file type logic
    #******************************************************************

    if file_extensions_contains_xls(ext_list) or file_extensions_contains_xlsx(ext_list):
        log_pusher.send_log('detected xls files','ok')

        if not 'xls_sheets_files' in cfg_settings:
            xlsSniffer = XLSSniffer(fname_list,log_pusher)
            cfg_settings['xls_sheets_files'] = xlsSniffer.dict_xls_sheets

        sheets = cfg_settings['xls_sheets_files']

        if not 'xls_sheets_sel' in cfg_settings:
            # if only have one sheet, autogen settings
            if np.all([sheet['sheets_count']==1 for sheet in sheets.values()]):
                log_pusher.send_log('single sheet xls mode','ok')
                cfg_settings['xls_sheets_sel_mode'] = 'idx_global'
                cfg_settings['xls_sheets_sel'] = 0
            else:
                return {'status':'need_xls_sheets','sheets':sheets, 'settings':cfg_settings}

        cfg_sheets = cfg_settings['xls_sheets_sel']

        if 'remove_blank_cols' not in cfg_settings:
            cfg_settings['remove_blank_cols'] = False
        if 'remove_blank_rows' not in cfg_settings:
            cfg_settings['remove_blank_rows'] = False
        if 'collapse_header' not in cfg_settings:
            cfg_settings['collapse_header'] = False

        if not 'xls_sheets_sel_fnames' in cfg_settings or cfg_settings['xls_sheets_sel_processed']!=cfg_settings['xls_sheets_sel']:

            converter = XLStoCSVMultiFile(fname_list, cfg_settings['xls_sheets_sel_mode'], cfg_settings['xls_sheets_sel'], log_pusher)
            fnames_converted = converter.convert_all(cfg_settings['remove_blank_cols'], cfg_settings['remove_blank_rows'],
                                                     cfg_settings['collapse_header'], cfg_settings.get('header_xls_range'),
                                                     cfg_settings.get('header_xls_start'), cfg_settings.get('header_xls_end'))

            # update settings
            cfg_settings['xls_sheets_sel_fnames'] = fnames_converted
            cfg_settings['xls_sheets_sel_processed']=cfg_settings['xls_sheets_sel']
            cfg_settings['csv_sniff']={"delim": ",", "skiprows": 0, "header": 0}

        fname_list = cfg_settings['xls_sheets_sel_fnames']

    elif file_extensions_contains_csv(ext_list):
        log_pusher.send_log('detected csv files','ok')

        # data for confirm settings
        log_pusher.send_log('scanning csv settings','ok')
        if not 'csv_sniff' in cfg_settings:
            cfg_settings['csv_sniff'] = sniff_settings_csv(fname_list)

        log_pusher.send_log('csv_sniff '+str(cfg_settings['csv_sniff']),'ok')

    else:        
        return {'status':'error','msg-error':'invalid file extension, can only  process .xls,.xlsx,.csv,.txt','settings':cfg_settings}


    #******************************************************************
    # combiner
    #******************************************************************

    # data for select columns
    #combiner = CombinerCSV(fname_list=fname_list, all_strings=True, sep=cfg_settings['csv_sniff']['delim'], header_row=cfg_settings['csv_sniff']['header'], skiprows=cfg_settings['csv_sniff']['skiprows'], nrows_preview=5, logger=log_pusher)
    combiner = CombinerCSV(fname_list=fname_list, all_strings=True, sep=cfg_settings['csv_sniff']['delim'],
                           read_csv_params={'header': cfg_settings['csv_sniff']['header'],
                                            'skiprows': cfg_settings['csv_sniff']['skiprows']},
                           nrows_preview=5, logger=log_pusher)

    if not 'columns_select_mode' in cfg_settings:
        log_pusher.send_log('determine columns mode','ok')
        
        col_preview = combiner.preview_columns()
        # todo: cache the preview dfs somehow? reading the same in next step
        
        cfg_settings['columns']={}
        cfg_settings['columns']['col_files'] = col_preview['files_columns']
        cfg_settings['columns']['col_all'] = col_preview['columns_all']
        cfg_settings['columns']['col_common'] = col_preview['columns_common']
        cfg_settings['columns']['is_columns_all_equal'] = col_preview['is_all_equal']
        df_col_present_json = col_preview['df_columns_present'].reset_index(drop=True).to_json(orient='records')
        df_col_order_json = col_preview['df_columns_order'].reset_index(drop=True).to_json(orient='records')

        if not col_preview['is_all_equal']:
            log_pusher.send_log('column mismatch detected','ok')
            col_data = column_mismatch_dict(col_preview['files_columns'])
            cfg_settings['columns_all_equal'] = False
            return {**{'status': 'need_columns', 'settings': cfg_settings}, **col_data}
        else:
            log_pusher.send_log('all columns equal','ok')
            cfg_settings['columns_select_mode']='all'
            cfg_settings['columns_all_equal'] = True


    # data for preview data
    if cfg_settings['columns_select_mode'] == 'all':
        cfg_col = cfg_settings['columns']['col_all']
    elif cfg_settings['columns_select_mode'] == 'common':
        cfg_col = cfg_settings['columns']['col_common']
    elif cfg_settings['columns_select_mode'] == 'manual':
        raise NotImplementedError
    else:
        raise ValueError('invalid columns_select_mode')
        
    combiner2 = CombinerCSVAdvanced(combiner, cfg_col)
    df_all_preview = combiner2.preview_combine()

    # data for combined data
    if cfg_return_df:
        df_all = combiner2.combine() 
        return {'status':'complete','data':df_all, 'preview_details':preview_dict(df_all_preview), 'settings':cfg_settings}
    else:
        fname_out_all = fname_out_folder + '/' + fname_out_base + '.csv'
        combiner2.combine_save(fname_out_all) 
        fname_out_sample = fname_out_folder + '/' + fname_out_base + '-sample.csv'
        combiner2.combine_preview_save(fname_out_sample) 
    
    log_pusher.send_log('preparing results','ok')
#        return combined_preview(df_all, df_all_preview, , cfg_settings, cfg_is_xls=False, cfg_return_df=cfg_return_df)

    return {'status':'complete', 'fname':fname_out_all, 'fname_sample':fname_out_sample, 'preview_details':preview_dict(df_all_preview), 'settings':cfg_settings}

