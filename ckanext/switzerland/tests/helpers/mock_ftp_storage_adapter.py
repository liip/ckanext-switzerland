import os

from ckanext.switzerland.harvester.ftp_storage_adapter import FTPStorageAdapter
from ckanext.switzerland.harvester.storage_adapter_factory import (
    STORAGE_ADAPTER_KEY,
    StorageAdapterFactory,
)


class MockFTPStorageAdapter(FTPStorageAdapter):
    def __init__(self, config_resolver, config, remotefolder=""):
        super(MockFTPStorageAdapter, self).__init__(
            config_resolver, config, remotefolder
        )
        self.cwd = "/"

    def _connect(self):
        pass

    def _disconnect(self):
        pass

    def cdremote(self, remotedir=None):
        if not remotedir:
            remotedir = self.remote_folder
        self.cwd = remotedir

    def get_remote_filelist(self, folder=None):
        if folder is None:
            folder = self.cwd
        return self.filesystem.listdir(folder)

    def get_remote_dirlist(self, folder=None):
        if folder is None:
            folder = self.cwd
        return self.filesystem.listdir(folder)

    def get_modified_date(self, filename, folder=None):
        if folder is None:
            folder = self.cwd
        return self.filesystem.getinfo(os.path.join(folder, filename)).get(
            "details", "modified"
        )

    def fetch(self, filename, localpath=None):
        if not localpath:
            localpath = os.path.join(self._config["localpath"], filename)

        localfile = open(localpath, "wb")

        content = self.filesystem.readbytes(os.path.join(self.cwd, filename))
        localfile.write(content)
        localfile.close()
        return "226 Transfer complete"


class MockStorageAdapterFactory(StorageAdapterFactory):
    def __init__(self, config_resolver):
        super(MockStorageAdapterFactory, self).__init__(config_resolver)

    def get_storage_adapter(self, remote_folder, config):
        if self.__is_legacy_config__(config):
            return MockFTPStorageAdapter(self.config_resolver, config, remote_folder)

        storage_adapter = config[STORAGE_ADAPTER_KEY].lower()

        # if storage_adapter == "s3":
        #     return S3StorageAdapter(self.config_resolver, config, remote_folder)

        if storage_adapter == "ftp":
            return MockFTPStorageAdapter(self.config_resolver, config, remote_folder)

        raise Exception("This type of storage is not supported: " + storage_adapter)
