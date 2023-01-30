from ftp_storage_adapter import FTPStorageAdapter
from s3_storage_adapter import S3StorageAdapter

STORAGE_ADAPTER_KEY = 'storage_adapter'
class StorageAdapterFactory(object):
    config_resolver = None

    def __init__(self, config_resolver):
        if not config_resolver:
            raise Exception('Cannot create adapter without config resolver')
        self.config_resolver = config_resolver

    def __is_legacy_config__(self, config):
        return STORAGE_ADAPTER_KEY not in config

    def get_storage_adapter(self, remote_folder, config):
        if self.__is_legacy_config__(config):
            return FTPStorageAdapter(self.config_resolver, config, remote_folder)

        storage_adapter = config[STORAGE_ADAPTER_KEY].lower()

        if storage_adapter == 's3':
            return S3StorageAdapter(self.config_resolver, config, remote_folder)
        
        if storage_adapter == 'ftp':
            return FTPStorageAdapter(self.config_resolver, config, remote_folder)
        
        raise Exception('This type of storage is not supported: ' + storage_adapter)