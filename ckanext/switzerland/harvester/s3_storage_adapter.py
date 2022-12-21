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
from storage_adapter_interface import StorageAdapterInterface

class S3StorageAdapter(StorageAdapterInterface):

    remote_folder = None
    config = None

    def __init__(self, config, remote_folder=''):
        if config is None:
            raise Exception("The storage adapter cannot be initialized without config")

        self.remote_folder = remote_folder.rstrip('/')
        self.config = config