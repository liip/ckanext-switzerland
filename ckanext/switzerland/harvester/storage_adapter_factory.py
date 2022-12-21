from ftp_helper import FTPHelper


class StorageAdapterFactory(object):
    def get_storage_adapter(self, remote_folder, config):
        ftp_config = {}
        ftp_config['ftp_server'] = config.get('ftp_server')
        return FTPHelper(remote_folder, config=config)