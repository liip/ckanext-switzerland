"""
S3 Storage Adapter
==================

Methods that help with dealing with remote AWS S3 Storage and local folders.
The class is intended to be used with Python's `with` statement, e.g.
`
    with S3StorageAdapter('/remote-base-path/', config) as storage_adapter:
        ...
`
"""
import logging
import boto3
import boto3.session
from storage_adapter_base import StorageAdapterBase
from aws_keys import (
    AWS_SECRET_KEY, 
    AWS_ACCESS_KEY, 
    AWS_REGION_NAME,
    AWS_BUCKET_NAME,
    AWS_RESPONSE_CONTENT,
    AWS_RESPONSE_PREFIXES
)

log = logging.getLogger(__name__)
class S3StorageAdapter(StorageAdapterBase):
    _aws_session = None
    _aws_client = None
    _working_directory = ''

    def __init__(self, config, remote_folder=''):
        if config is None:
            raise Exception("The storage adapter cannot be initialized without config")

        self.remote_folder = remote_folder.rstrip('/')
        self._config = config

        self.create_local_dir()

    def __enter__(self):
        self._connect()
        self.cdremote()
        return self

    def __exit__(self, type, value, traceback):
        pass
    
    def _connect(self):
        self._aws_session = boto3.session.Session(
            aws_access_key_id=self._config[AWS_ACCESS_KEY],
            aws_secret_access_key=self._config[AWS_SECRET_KEY],
            region_name=self._config[AWS_REGION_NAME]
        )

        self._aws_client = self._aws_session.client('s3')
    
    def _disconnect(self):
        # as boto3 is HTTP call based, we don't need to close anything
        pass

    def cdremote(self, remotedir=None):
        # there is no such command on S3. We just need to keep a ref to a Working Directory
        self._working_directory = remotedir.rstrip('/').lstrip('/') if remotedir is not None else ''

    def get_top_folder(self):
        # the top folder is basically just the name of the bucket.
        return self._config[AWS_BUCKET_NAME]

    def get_remote_filelist(self, folder=None):
        # files are stored flat on AWS. We use the Prefix parameter to filter the results
        all_in_folder = self.get_remote_dirlist(folder)
        only_files = filter(lambda name : not name.endswith('/') and name, all_in_folder)
        return only_files

    def get_remote_dirlist(self, folder=None):  
        prefix = self._working_directory 
        prefix = prefix + '/' if prefix != "" else ""
        s3_objects = self._aws_client.list_objects(Bucket=self._config[AWS_BUCKET_NAME], Prefix=prefix, Delimiter="/")
        if not s3_objects or AWS_RESPONSE_CONTENT not in s3_objects:
            log.info("Listing files on AWS returned an empty list")
            return []
        
        objects = map(lambda object : object['Key'], s3_objects[AWS_RESPONSE_CONTENT])

        if AWS_RESPONSE_PREFIXES in s3_objects:
            objects.extend(map(lambda object : object['Prefix'], s3_objects[AWS_RESPONSE_PREFIXES]))
        
        without_prefix = map(lambda file :  file.lstrip(prefix), objects)
        
        without_prefix.sort()

        return without_prefix