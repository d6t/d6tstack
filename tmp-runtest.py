import glob
import d6tstack.combine_csv
import importlib
importlib.reload(d6tstack.combine_csv)

fname_list=glob.glob('test-data/input/test-data-input-csv-clean-*.csv')
fname_list=glob.glob('test-data/input/test-data-input-csv-colmismatch-*.csv')
fname_list=glob.glob('test-data/input/test-data-input-csv-reorder-*.csv')
combiner = d6tstack.combine_csv.CombinerCSV(fname_list)
combiner.is_column_present().all().values.tolist()
combiner.is_column_present_common()
combiner.sniff_results['df_columns_order']['profit'].values.tolist()

'''
todo:
write tests

'''
