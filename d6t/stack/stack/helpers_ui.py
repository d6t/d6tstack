from .helpers import *
import ntpath
from itertools import groupby

#******************************************************************
# combine helpers
#******************************************************************
def column_mismatch_dict(fname_col):
    col_common = list_common(fname_col.values())
    col_common_len = len(col_common)
    col_unique = list_unique(fname_col.values())
    col_unique_len = len(col_unique)

    df_col_info = []
    for key, value in fname_col.items():
        col_info = {}
        col_info['fname'] = ntpath.basename(key)
        col_info['col_common_names'] = list(col_common)
        col_info['col_common_count'] = col_common_len
        col_info['col_unique_names'] = list(set(value).difference(set(col_common)))
        col_info['col_unique_count'] = len(col_info['col_unique_names'])
        df_col_info.append(col_info)

    # all info
    col_info = {}
    col_info['fname'] = 'all'
    col_info['col_common_names'] = list(col_common)
    col_info['col_common_count'] = col_common_len
    col_info['col_unique_names'] = list(set(value).difference(set(col_common)))
    col_info['col_unique_count'] = len(col_info['col_unique_names'])

    return {'columns_files':df_col_info,'columns_files_all':col_info}


def preview_dict(df):
    values = df.values.tolist()
    rows = {}
    for key, group in groupby(values, lambda x: x[0]):
        rows[key] = [i[1:] for i in list(group)]

    return {'columns':df.columns.tolist(), 'rows': rows}


def combined_preview(df_all, df_all_preview, fname_out, cfg_settings, cfg_is_xls, cfg_return_df):
    df_all_preview_dict = preview_dict(df_all_preview)

    if cfg_return_df:
        return {'status':'complete','data':df_all, 'preview_details':df_all_preview_dict, 'settings':cfg_settings}
    else:
        if not os.path.exists(os.path.dirname(fname_out)):
            os.makedirs(os.path.dirname(fname_out))
        if cfg_is_xls:
            fname_out_all = fname_out + '.xlsx'
            fname_out_sample = fname_out + '-sample.xlsx'
            df_all.to_excel(fname_out_all, index=False)
            df_all_preview.to_excel(fname_out_sample)
        else:
            fname_out_all = fname_out + '.csv'
            fname_out_sample = fname_out + '-sample.csv'
            df_all.to_csv(fname_out_all, index=False)
            df_all_preview.to_csv(fname_out_sample, index=False)

        return {'status':'complete', 'fname':fname_out_all, 'fname_sample':fname_out_sample, 'preview_details':df_all_preview_dict, 'settings':cfg_settings}


