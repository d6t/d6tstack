from moto import mock_s3
import shutil
import os
import pytest
from d6tstack.sync import FTPSync

cfg_ftp_host = 'ftp.fic.com.tw'
cfg_ftp_usr = 'anonymous'
cfg_ftp_pwd = 'random'
cfg_ftp_dir_base = '/photo/ia/'
local_dir = '/tmp/new_data/'
ftp_file_path = 'aquapad/AquaPad.jpg'


def _remove_local_files(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


def test_sync_local():
    _remove_local_files(local_dir)
    ftpsync = FTPSync(cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd, cfg_ftp_dir_base,
                      local_dir=local_dir, logger=None)

    # Local files should be empty
    assert ftpsync.get_all_files().tolist() == []

    # FTP files should be there
    assert ftpsync.get_all_files(ftp=True).tolist() == [ftp_file_path]

    # Get files for sync local
    assert ftpsync.get_files_for_sync() == ({ftp_file_path}, 278683)


@mock_s3
def test_sync_s3():
    ftpsync = FTPSync(cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd, cfg_ftp_dir_base,
                      local_dir=local_dir, logger=None)

    # s3 files error for no connection details
    with pytest.raises(ValueError) as e:
        ftpsync.get_s3_files()

    ftpsync = FTPSync(cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd, cfg_ftp_dir_base,
                      cfg_s3_key="test", cfg_s3_secret="test", bucket_name="test",
                      local_dir=local_dir, logger=None)

    assert ftpsync.get_s3_files() == set()

    file_key = "test/hp.jpg"
    with pytest.raises(FileNotFoundError) as e:
        ftpsync.upload_to_s3(file_key, "test-data/syncasdsadf/hp.jpg")

    ftpsync.upload_to_s3(file_key, "test-data/sync/hp.jpg")
    assert ftpsync.get_s3_files() == {file_key}

    assert ftpsync.get_files_for_sync(to_s3=True) == ({ftp_file_path}, 278683)

    ftpsync.upload_ftp_files(to_s3=True)
    assert ftpsync.get_s3_files() == {ftp_file_path, file_key}
