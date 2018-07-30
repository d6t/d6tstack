import zipfile
import glob
import os

if not os.path.exists('test-data/output/__init__.py'):
	fhandle = open('test-data/output/__init__.py', 'w')
	fhandle.close()


ziphandle = zipfile.ZipFile('test-data.zip', 'w')
cfg_path_base = 'test-data/input/test-data-input'
for fname in  glob.glob(cfg_path_base+'*.csv')+glob.glob(cfg_path_base+'*.xls')+glob.glob(cfg_path_base+'*.xlsx'):
	ziphandle.write(fname)

ziphandle.write('test-data/output/__init__.py')
ziphandle.close()
