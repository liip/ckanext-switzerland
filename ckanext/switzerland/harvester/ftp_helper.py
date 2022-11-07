"""
FTP Helper
==================

Methods that help with dealing with remote ftp and local folders.
The class is intended to be used with Python's `with` statement, e.g.
`
    with FTPHelper('/remote-base-path/') as ftph:
        ...
`
"""

import logging
from pprint import pformat

import os

import pysftp
from ckan.plugins.toolkit import config as ckanconf
import ftplib
import zipfile
import errno
import datetime
import ssl

log = logging.getLogger(__name__)


class FTPHelper(object):
    """ FTP Helper Class """

    _config = None

    ftps = None

    remotefolder = ''

    tmpfile_extension = '.TMP'

    # tested
    def __init__(self, remotefolder='', config=None):
        """
        Load the ftp configuration from ckan config file

        :param remotefolder: Remote folder path
        :type remotefolder: str or unicode
        """

        if config:
            # read ftpconfig from harvester-config
            ftpconfig = config
            # all server related information is read from the ckan-config
            ftp_server_ = 'ckan.ftp.' + ftpconfig['ftp_server']
            for key in ['username', 'password', 'keyfile', 'host', 'port', 'remotedirectory', 'localpath']:
                ftpconfig[key] = ckanconf.get(ftp_server_+'.%s' % key, '')
        else:
            raise Exception('The ftp server must be specified in the harvester configuration')

        ftpconfig['host'] = str(ftpconfig['host'])
        ftpconfig['port'] = str(ftpconfig['port'])

        log.info('Using FTP-Config: %s' % pformat(config))
        
        self._config = ftpconfig
        # prepare the remote path
        self.remotefolder = remotefolder.rstrip("/")
        # create the local directory, if it does not exist
        self.create_local_dir()

    # tested
    def __enter__(self):
        """
        Establish an ftp connection and cd into the configured remote directory

        :returns: Instance of FTPHelper
        :rtype: FTPHelper
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
        return "%s:%d" % (self._config['host'], self._config['port'])

    # tested
    def _mkdir_p(self, path, perms=0777):
        """
        Recursively create local directories
        Based on http://stackoverflow.com/a/600612/426266

        :param path: Folder path
        :type path: str or unicode
        :param perms: Folder permissions
        :type perms: octal
        """
        try:
            os.makedirs(path, perms)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                # path already exists
                pass
            else:
                # something went wrong with the creation of the directories
                raise

    # tested
    def create_local_dir(self, folder=None):
        """
        Create a local folder

        :param folder: Folder path
        :type folder: str or unicode

        :returns: None
        :rtype: None
        """
        if not folder:
            folder = self._config['localpath']
        # create the local directory if it does not exist

        folder = folder.rstrip("/")

        if not os.path.isdir(folder):
            self._mkdir_p(folder)
            log.debug("Created folder: %s" % str(folder))

    # tested
    def _connect(self):
        """
        Establish an FTP connection
        ftps - to connect with password
        sftp - to connect with keyfile
        :returns: None
        :rtype: None
        """

        if self._config['password']:
            # overwrite the default port (21)
            ftplib.FTP.port = int(self._config['port'])
            # we need to set the TLS version explicitly to allow connection
            # to newer servers who have disabled older TLS versions (< TLSv1.2)
            ftplib.FTP_TLS.ssl_version = ssl.PROTOCOL_TLSv1_2
            # connect
            self.ftps = ftplib.FTP_TLS(self._config['host'], self._config['username'], self._config['password'])
            # switch to secure data connection
            self.ftps.prot_p()
        elif self._config['keyfile']:
            # connecting via SSH
            self.sftp = pysftp.Connection(host=self._config['host'],
                                          username=self._config['username'],
                                          private_key=self._config['keyfile'],
                                          port=int(self._config['port']),
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
            remotedir = self.remotefolder
        if self.ftps:
            self.ftps.cwd(remotedir)
        elif self.sftp:
            self.sftp.cwd(remotedir)

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
            files = self.sftp.listdir(self.remotefolder)

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
                dirlist = self.sftp.listdir(self.remotefolder)

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
            folder = self.remotefolder
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

    # tested
    def get_local_dirlist(self, localpath="."):
        """
        Get directory listing, including all sub-folders

        :param localpath: Path to a local folder
        :type localpath: str or unicode

        :returns: Directory listing
        :rtype: list
        """
        dirlist = []
        for dirpath, dirnames, filenames in os.walk(localpath):
            for filename in [f for f in filenames]:
                dirlist.append(os.path.join(dirpath, filename))
        return dirlist

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

    def get_local_path(self):
        return self._config['localpath']

    # tested (with empty dir)
    def is_empty_dir(self, folder=None):
        """
        Check if a remote directory is empty

        :param folder: Folder name or path
        :type folder: str or unicode

        :returns: Number of files or directories in remote folder
        :rtype: int
        """
        if not folder:
            folder = None
        num_files = len(self.get_remote_dirlist_all(folder))
        return num_files

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
            localpath = os.path.join(self._config['localpath'], filename)

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

    # tested
    def unzip(self, filepath):
        """
        Extract a single zip file
        E.g. will extract a file /tmp/somedir/myfile.zip into /tmp/somedir/

        :param filepath: Path to a local file
        :type filepath: str or unicode

        :returns: Number of extracted files
        :rtype: int
        """
        na, file_extension = os.path.splitext(filepath)
        if file_extension.lower() == '.zip':
            log.info("Unzipping: %s" % filepath)
            target_folder = os.path.dirname(filepath)
            zfile = zipfile.ZipFile(filepath)
            filelist = zfile.namelist()
            zfile.extractall(target_folder)
            return len(filelist)
