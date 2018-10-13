from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os
import ftputil


def get_ftp_files():
    fileSetftp = set()
    with ftputil.FTPHost(cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd) as ftp_host:
        ftp_host.use_list_a_option = False
        for dir_, _, files in ftp_host.walk(cfg_dir_ftp):
            for fileName in files:
                relDir = os.path.relpath(dir_, cfg_dir_ftp)
                relFile = os.path.join(relDir, fileName)
                fileSetftp.add(relFile)
    return fileSetftp


def upload_ftp_files_s3(ftp_files, s3_files, bucket):
    files_ftp_sync = set(ftp_files).difference(s3_files)
    with ftputil.FTPHost(cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd) as ftp_host:
        for ftp_file in files_ftp_sync:
            full_name = cfg_dir_ftp + ftp_file
            basename = os.path.basename(full_name)
            temp_path = '/tmp/'+basename
            ftp_host.download(full_name, temp_path)
            with open(temp_path, 'rb') as f:
                key = Key(bucket, ftp_file)
                key.set_contents_from_file(f)


def list_s3_files(bucket):
    s3_files = set()
    for key in bucket.list():
        s3_files.add(key.name.encode('utf-8'))
    return s3_files


def upload_to_s3(bucket):
    fname = '/home/anuj/Pictures/test/hp.jpg'
    basename = os.path.basename(fname)
    key = Key(bucket, basename)
    with open(fname, 'rb') as f:
        key.set_contents_from_file(f)


if __name__ == "__main__":
    print("S3 File sync")
    s3_id = ''
    s3_key = ''
    bucket_name = 'test-anuj-ftp-sync'

    cfg_ftp_host = 'ftp.fic.com.tw'
    cfg_ftp_usr = 'anonymous'
    cfg_ftp_pwd = 'random'
    cfg_dir_ftp = '/photo/ia/'

    s3_conn = S3Connection(s3_id, s3_key, host='s3.ap-south-1.amazonaws.com')
    bucket = s3_conn.get_bucket(bucket_name)
    s3_files = list_s3_files(bucket)
    upload_to_s3(bucket)

    ftp_files = get_ftp_files()
    print(ftp_files)

    upload_ftp_files_s3(ftp_files, s3_files, bucket)
