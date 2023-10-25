import os

from ckanext.switzerland.harvester.ftp_helper import FTPStorageAdapter


class MockFTPStorageAdapter(FTPStorageAdapter):
    def __init__(self, remotefolder=""):
        super(MockFTPStorageAdapter, self).__init__(remotefolder)
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
        return self.filesystem.listdir(folder, files_only=True)

    def get_remote_dirlist(self, folder=None):
        if folder is None:
            folder = self.cwd
        return self.filesystem.listdir(folder, dirs_only=True)

    def get_modified_date(self, filename, folder=None):
        if folder is None:
            folder = self.cwd
        return self.filesystem.getinfo(os.path.join(folder, filename))["modified_time"]

    def fetch(self, filename, localpath=None):
        if not localpath:
            localpath = os.path.join(self._config["localpath"], filename)

        localfile = open(localpath, "wb")

        content = self.filesystem.getcontents(os.path.join(self.cwd, filename))
        localfile.write(content)
        localfile.close()
        return "226 Transfer complete"
