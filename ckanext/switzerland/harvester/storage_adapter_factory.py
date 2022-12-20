from ftp_helper import FTPHelper


class StorageAdapterFactory(object):
    def get_storage_adapter(self, remote_folder, config):
        return FTPHelper(remote_folder, config=config)