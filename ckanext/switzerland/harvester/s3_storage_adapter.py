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
from aws_keys import AWS_SECRET_KEY, AWS_ACCESS_KEY, AWS_REGION_NAME

class S3StorageAdapter(StorageAdapterBase):
    #TODO: Move up
    remote_folder = None
    _aws_session = None

    def __init__(self, config, remote_folder=''):
        if config is None:
            raise Exception("The storage adapter cannot be initialized without config")

        self.remote_folder = remote_folder.rstrip('/')
        self._config = config

        self.create_local_dir()
    
    def _connect(self):
        """
        Establish an Session with AWS, and stores it in _client variable
        :returns: None
        :rtype: None
        """
        self._aws_session = boto3.session.Session(
            aws_access_key_id=self._config[AWS_ACCESS_KEY],
            aws_secret_access_key=self._config[AWS_SECRET_KEY],
            region_name=self._config[AWS_REGION_NAME]
        )