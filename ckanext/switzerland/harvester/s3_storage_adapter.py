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
    _aws_session = None
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
        """
        Establish an Session with AWS, and stores it in _aws_session variable
        :returns: None
        :rtype: None
        """
        self._aws_session = boto3.session.Session(
            aws_access_key_id=self._config[AWS_ACCESS_KEY],
            aws_secret_access_key=self._config[AWS_SECRET_KEY],
            region_name=self._config[AWS_REGION_NAME]
        )
    
    def _disconnect(self):
        """
        As boto3 only make API call through HTTP, there is nothing to close
        :returns: None
        :rtype: None
        """
        pass

    def cdremote(self, remotedir=None):
        """
        Change remote directory

        :param remotedir: Full path on the remote server
        :type remotedir: str or unicode

        :returns: None
        :rtype: None
        """
        self._working_directory = remotedir.rstrip('/') if remotedir is not None else '/'
        