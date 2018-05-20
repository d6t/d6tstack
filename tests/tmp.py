import importlib
import d6tstack.utils
importlib.reload(d6tstack.utils)

cfg_path_folder_base = '/mnt/data/data.raw/travelclick/'
cfg_path = cfg_path_folder_base+'predict/STR Rolling Weekly Since 9-11-01 to 4-14-18 values weekly.xlsx'

# all
df_str=d6tstack.utils.read_excel_advanced(cfg_path, header_xls_start="A5", header_xls_end="D7")
df_str.head()
