from ftp_storage_adapter import FTPStorageAdapter


class StorageAdapterFactory(object):
    def get_storage_adapter(self, remote_folder, config):
        ftp_config = {}
        ftp_config['ftp_server'] = config.get('ftp_server')
        return FTPStorageAdapter(remote_folder, config=config)