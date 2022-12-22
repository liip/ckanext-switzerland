from ftp_storage_adapter import FTPStorageAdapter
from s3_storage_adapter import S3StorageAdapter

STORAGE_ADAPTER_KEY = 'storage_adapter'
class StorageAdapterFactory(object):
    def __is_legacy_config__(self, config):
        return STORAGE_ADAPTER_KEY not in config

    def get_storage_adapter(self, config_resolver, remote_folder, config):
        if self.__is_legacy_config__(config):
            return FTPStorageAdapter(config_resolver, remote_folder, config=config)

        storage_adapter = config[STORAGE_ADAPTER_KEY]

        if storage_adapter == 'S3':
            return S3StorageAdapter(config, remote_folder)
        
        if storage_adapter == 'FTP':
            return FTPStorageAdapter(config_resolver, remote_folder, config=config)
        
        raise Exception('This type of storage is not supported: ' + storage_adapter)