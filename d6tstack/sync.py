import boto3
import botocore
import os
import ftputil
import numpy as np


class FTPSync:
    """

        FTP Sync class. It allows users to sync their files to s3 or local.

        Args:
            cfg_ftp_host (string): FTP host name
            cfg_ftp_usr (string): FTP login username
            cfg_ftp_pwd (string): FTP login password
            cfg_ftp_dir (string): FTP starting directory to be used for sync.
            cfg_s3_key (string): AWS S3 key for connection
            cfg_s3_secret (string): AWS S3 secret for connection
            bucket_name (string): Bucket name in s3 for syncing the files
            local_dir (string): local dir path to be used for sync. dir will be created if not exist.
            logger (object): logger object with send_log()

        """
    def __init__(self, cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd, cfg_ftp_dir,
                 cfg_s3_key=None, cfg_s3_secret=None, bucket_name=None,
                 local_dir='./data/', logger=None):
        self.cfg_ftp_host = cfg_ftp_host
        self.cfg_ftp_usr = cfg_ftp_usr
        self.cfg_ftp_pwd = cfg_ftp_pwd
        self.cfg_ftp_dir = cfg_ftp_dir
        self.ftp_host = ftputil.FTPHost(cfg_ftp_host, cfg_ftp_usr, cfg_ftp_pwd)
        self.ftp_host.use_list_a_option = False
        self.s3_client = None
        self.bucket_name = None
        if cfg_s3_key and cfg_s3_secret and bucket_name:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=cfg_s3_key,
                aws_secret_access_key=cfg_s3_secret
            )
            exists = True
            try:
                self.s3_client.head_bucket(Bucket=bucket_name)
            except botocore.exceptions.ClientError as e:
                # If a client error is thrown, then check that it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    exists = False
            if not exists:
                if logger:
                    logger.send_log('Bucket does not exist. Creating bucket', 'ok')
                self.s3_client.create_bucket(Bucket=bucket_name)
            self.bucket_name = bucket_name
        self.local_dir = local_dir
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.logger = logger

    def get_all_files(self, subdirs=True, ftp=False):
        """

        Get all file list from local or ftp

        Args:
            subdirs (bool): return all the files in directory recursively? If `false` it will not go to sub directories
            ftp (bool): local files if `true` otherwise local files

        Returns:
            Alphabetically Sorted file list

        """
        fileSet = set()
        host = os
        from_dir = self.local_dir
        if ftp:
            host = self.ftp_host
            from_dir = self.cfg_ftp_dir
        if subdirs:
            for dir_, _, files in host.walk(from_dir):
                for fileName in files:
                    relDir = os.path.relpath(dir_, from_dir)
                    relFile = os.path.join(relDir, fileName)
                    fileSet.add(relFile)
        else:
            for fileName in host.listdir(from_dir):
                relFile = os.path.join(from_dir, fileName)
                if host.path.isfile(relFile):
                    fileSet.add(relFile)
        return np.sort(list(fileSet))

    def get_s3_files(self):
        """

            Get all file list from s3 in the given bucket

           Returns:
                File list from s3 in bucket

        """
        if not self.s3_client or not self.bucket_name:
            raise ValueError("S3 credentials are mandatory to use this functionality")
        s3_files = set()
        all_files = self.s3_client.list_objects(Bucket=self.bucket_name)
        for content in all_files.get('Contents', []):
            s3_files.add(content.get('Key'))
        return s3_files

    def upload_to_s3(self, fname, local_path):
        """

            Upload a single file from local to s3

            Args:
                fname (string): Filename in s3
                local_path (string): Local path of file to be uploaded

        """

        with open(local_path, 'rb') as f:
            self.s3_client.upload_fileobj(f, self.bucket_name, fname)

    def get_files_for_sync(self, subdirs=True, to_s3=False):
        """

            Get File list for sync along with total file size

            Args:
                subdirs (bool): return all the files in directory recursively? If `false` it will not go to sub directories, Optional
                to_s3 (bool): get files to be sync from ftp to local. If `true` all files will be synced from ftp to s3

        """
        ftp_files = self.get_all_files(subdirs=subdirs, ftp=True)
        if to_s3:
            server_files = self.get_s3_files()
        else:
            server_files = self.get_all_files(subdirs=subdirs)
        files_ftp_sync = set(ftp_files).difference(set(server_files))
        total_file_size = sum([self.ftp_host.path.getsize(os.path.join(self.cfg_ftp_dir, f))
                               for f in files_ftp_sync])
        return files_ftp_sync, total_file_size

    def upload_ftp_files(self, subdirs=True, to_s3=False):
        """

            Get File list for sync along with total file size

            Args:
                subdirs (bool): Upload files from ftp recursively? If `false` it will not go to sub directories, Optional
                to_s3 (bool): upload files from ftp to local. If `true` files will be uploaded from ftp to s3

        """

        files_ftp_sync, total_file_size = self.get_files_for_sync(subdirs=subdirs, to_s3=to_s3)
        for ftp_file in files_ftp_sync:
            full_name = os.path.join(self.cfg_ftp_dir, ftp_file)
            local_path = os.path.join(self.local_dir, ftp_file)
            file_dir_local = os.path.dirname(local_path)
            if not os.path.exists(file_dir_local):
                os.makedirs(file_dir_local)
            self.ftp_host.download(full_name, local_path)
            if to_s3:
                self.upload_to_s3(ftp_file, local_path)
