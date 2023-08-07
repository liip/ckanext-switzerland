"""
FTP Storage Adapter
==================

Methods that help with dealing with remote ftp/sftp and local folders.
The class is intended to be used with Python's `with` statement, e.g.
`
    with FTPStorageAdapter('/remote-base-path/') as storage:
        ...
`
"""

import logging

import os

from ckanext.switzerland.harvester.config.config_key import ConfigKey
from ckanext.switzerland.harvester.storage_adapter_base import StorageAdapterBase

import pysftp
import ftplib
import datetime
import ssl

from ckanext.switzerland.harvester.keys import (
    FTP_USER_NAME,
    FTP_PASSWORD,
    FTP_KEY_FILE,
    FTP_HOST,
    FTP_PORT,
    FTP_SERVER_KEY,
    LOCAL_PATH,
    REMOTE_DIRECTORY
)

log = logging.getLogger(__name__)

CONFIG_KEYS = [
    ConfigKey(FTP_USER_NAME, str, True),
    ConfigKey(FTP_PASSWORD, str, True),
    ConfigKey(FTP_KEY_FILE, str),
    ConfigKey(FTP_HOST, str, True),
    ConfigKey(FTP_PORT, int, True, lambda x: x > 0, 'Port should be a positive number'),
    ConfigKey(LOCAL_PATH, str, True),
    ConfigKey(REMOTE_DIRECTORY, str, True),
]
class FTPStorageAdapter(StorageAdapterBase):
    """ FTP Storage Adapter Class """

    ftps = None
    sftp = None
    tmpfile_extension = '.TMP'

    # tested
    def __init__(self, config_resolver, config, remote_folder=''):
        super(FTPStorageAdapter, self).__init__(
            config_resolver,
            config,
            remote_folder,
            FTP_SERVER_KEY,
            CONFIG_KEYS,
            'ckan.ftp'
        )

    # tested
    def __enter__(self):
        """
        Establish an ftp connection and cd into the configured remote directory

        :returns: Instance of FTPStorageAdapter
        :rtype: FTPStorageAdapter
        """
        self._connect()
        # cd into the remote directory
        self.cdremote()
        # return helper instance
        return self

    # tested
    def __exit__(self, type, value, traceback):
        """
        Disconnect the ftp connection
        """
        self._disconnect()

    # tested
    def get_top_folder(self):
        """
        Get the name of the top-most folder in /tmp

        :returns: The name of the folder created by ftplib, e.g. 'mydomain.com:21'
        :rtype: string
        """
        return "%s:%d" % (self._config[FTP_HOST], self._config[FTP_PORT])

    # tested
    def _connect(self):
        """
        Establish an FTP connection
        ftps - to connect to FTP Server using password
        sftp - to connect to SFTP Server using keyfile/password
        :returns: None
        :rtype: None
        """

        if self._config[FTP_PASSWORD]:
            # connect
            # check SFTP protocol is used, pysftp defaults to 22
            if int(self._config[FTP_PORT]) == 22:
                self.sftp = pysftp.Connection(host=self._config[FTP_HOST],
                                              username=self._config[FTP_USER_NAME],
                                              password=self._config[FTP_PASSWORD],
                                              port=int(self._config[FTP_PORT]),
                                              )
            else:
                # overwrite the default port (21)
                ftplib.FTP.port = int(self._config[FTP_PORT])
                # we need to set the TLS version explicitly to allow connection
                # to newer servers who have disabled older TLS versions (< TLSv1.2)
                ftplib.FTP_TLS.ssl_version = ssl.PROTOCOL_TLSv1_2
                self.ftps = ftplib.FTP_TLS(self._config[FTP_HOST],
                                           self._config[FTP_USER_NAME],
                                           self._config[FTP_PASSWORD])
                # switch to secure data connection
                self.ftps.prot_p()
        elif self._config[FTP_KEY_FILE]:
            # connecting via SSH
            self.sftp = pysftp.Connection(host=self._config[FTP_HOST],
                                          username=self._config[FTP_USER_NAME],
                                          private_key=self._config[FTP_KEY_FILE],
                                          port=int(self._config[FTP_PORT]),
                                          )
    # tested
    def _disconnect(self):
        """
        Close ftp connection

        :returns: None
        :rtype: None
        """
        if self.ftps:
            self.ftps.quit()  # '221 Goodbye.'
        elif self.sftp:
            self.sftp.close()

    # tested
    def cdremote(self, remotedir=None):
        """
        Change remote directory

        :param remotedir: Full path on the remote server
        :type remotedir: str or unicode

        :returns: None
        :rtype: None
        """
        if not remotedir:
            remotedir = self.remote_folder
        if self.ftps:
            self.ftps.cwd(remotedir)
        elif self.sftp:
            self.sftp.chdir(remotedir)

    def get_remote_filelist(self, folder=None):
        """
        List files in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        cmd = 'MLSD'
        if folder:
            cmd += ' ' + folder

        files = []
        if self.ftps:
            files_dirs = []
            self.ftps.retrlines(cmd, files_dirs.append)
            for file_dir in files_dirs:
                data, filename = file_dir.split(' ', 1)
                for kv in filter(lambda x: x, data.split(';')):
                    key, value = kv.split('=')
                    if key == 'type' and value == 'file':
                        files.append(filename)
        elif self.sftp:
            files = self.sftp.listdir(self.remote_folder)

        return files

    # tested
    def get_remote_dirlist(self, folder=None):
        """
        List files and sub-directories in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """

        # get dir listing of a specific directory
        if folder:
            if self.ftps:
                dirlist = self.ftps.nlst(folder)
            elif self.sftp:
                dirlist = self.sftp.listdir(folder)
        # get dir listing of current directory
        else:
            if self.ftps:
                dirlist = self.ftps.nlst()
            elif self.sftp:
                dirlist = self.sftp.listdir(self.remote_folder)

        # filter out '.' and '..' and return the list
        dirlist = filter(lambda entry: entry not in ['.', '..'], dirlist)

        # .TMP must be ignored, as they are still being uploaded
        dirlist = [x for x in dirlist if not x.lower().endswith(self.tmpfile_extension.lower())]

        return dirlist

    # see: http://stackoverflow.com/a/31512228/426266
    def get_remote_dirlist_all(self, folder=None):
        """
        Get a listing of all files (including subdirectories in a specific folder on the remote server

        :param folder: Folder name or path
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        if not folder:
            folder = self.remote_folder
        dirs = []
        new_dirs = self.get_remote_dirlist(folder)
        while len(new_dirs) > 0:
            for directory in new_dirs:
                dirs.append(directory)
            old_dirs = new_dirs[:]
            new_dirs = []
            for directory in old_dirs:
                for new_dir in self.get_remote_dirlist(directory):
                    new_dirs.append(new_dir)
        dirs.sort()
        return dirs

    def get_modified_date(self, filename, folder=None):
        """
        Get the last modified date of a remote file

        :param filename: Filename of remote file to check
        :type filename: str or unicode
        :param folder: Remote folder
        :type folder: str or unicode

        :returns: Date
        :rtype: TODO
        """
        modified_date = None

        if folder:
            self.cdremote(folder)
        if self.ftps:
            ret = self.ftps.sendcmd('MDTM %s' % filename)
            # example: '203 20160621123722'
        elif self.sftp:
            ret = self.sftp.stat(filename).st_mtime
            # example unix time (int): 1667384100

        if ret:
            if self.ftps:
                modified_date = ret.split(' ')[1]
                # example: '20160621123722'

                modified_date = datetime.datetime.strptime(modified_date, '%Y%m%d%H%M%S')
                # example: 2022-11-02 19:07:13
            elif self.sftp:
                modified_date = datetime.datetime.fromtimestamp(ret)
                # example: 2022-11-02 13:46:07

        log.debug('modified date of %s: %s ' % (filename, str(modified_date)))

        return modified_date

    # tested
    def fetch(self, filename, localpath=None):
        """
        Fetch a single file from the remote server with ftplib and pysftp

        :param filename: File to fetch
        :type filename: str or unicode
        :param localpath: Local folder to store the file
        :type localpath: str or unicode

        :returns: Status of the FTP operation
        :rtype: string
        """
        if not localpath:
            localpath = os.path.join(self._config[LOCAL_PATH], filename)

        localfile = open(localpath, 'wb')

        if self.ftps:
            status = self.ftps.retrbinary('RETR %s' % filename, localfile.write)
            localfile.close()
        elif self.sftp:
            status = self.sftp.get(filename, localpath=localpath)

            if status == None:
                status = "226 Transfer complete"
            else:
                # something went wrong with copies a file between the remote host and the local host
                raise

        return status

