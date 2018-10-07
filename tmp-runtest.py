import glob
import d6tstack.combine_csv
import importlib
importlib.reload(d6tstack.combine_csv)

fname_list=glob.glob('test-data/input/test-data-input-csv-clean-*.csv')
fname_list=glob.glob('test-data/input/test-data-input-csv-reorder-*.csv')
fname_list=glob.glob('test-data/input/test-data-input-csv-colmismatch-*.csv')
# combiner = d6tstack.combine_csv.CombinerCSV(fname_list)
# combiner.is_column_present().all().values.tolist()
# combiner.is_column_present_common()
# combiner.sniff_results['df_columns_order']['profit'].values.tolist()

importlib.reload(d6tstack.combine_csv)
combiner = d6tstack.combine_csv.CombinerCSV(fname_list, columns_rename={'profit2':'profit_new'})
combiner.to_pandas()

'''
todo:
write tests

'''