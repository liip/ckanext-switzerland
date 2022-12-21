import os
import logging
import errno

log = logging.getLogger(__name__)
class StorageAdapterBase(object):
    _config = None
    remote_folder = None

    def _connect(self):
        raise NotImplementedError('_connect')
    
    def _disconnect(self):
        raise NotImplementedError('_disconnect')

    # tested
    def __enter__(self):
        """
        Establishes a connection to the Storage
        """
        raise NotImplementedError('__enter__')

    # tested
    def __exit__(self, type, value, traceback):
        """
        Closes a connection to the Storage
        """
        raise NotImplementedError('__exit__')
    
    def get_top_folder(self):
        """
        Get the name of the top-most folder in /tmp

        :returns: The name of the folder created by ftplib, e.g. 'mydomain.com:21'
        :rtype: string
        """
        raise NotImplementedError('get_top_folder')

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

    def cdremote(self, remotedir=None):
        """
        Change remote directory

        :param remotedir: Full path on the remote server
        :type remotedir: str or unicode

        :returns: None
        :rtype: None
        """
        pass

    def get_remote_filelist(self, folder=None):
        """
        List files in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        pass

    def get_remote_dirlist(self, folder=None):
        """
        List files and sub-directories in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        pass

    def get_remote_dirlist_all(self, folder=None):
        """
        Get a listing of all files (including subdirectories in a specific folder on the remote server

        :param folder: Folder name or path
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        pass

    def get_local_dirlist(self, localpath="."):
        """
        Get directory listing, including all sub-folders

        :param localpath: Path to a local folder
        :type localpath: str or unicode

        :returns: Directory listing
        :rtype: list
        """
        pass

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
        pass

    def get_local_path(self):
        pass


    def is_empty_dir(self, folder=None):
        """
        Check if a remote directory is empty

        :param folder: Folder name or path
        :type folder: str or unicode

        :returns: Number of files or directories in remote folder
        :rtype: int
        """
        pass

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
        pass

    def unzip(self, filepath):
        """
        Extract a single zip file
        E.g. will extract a file /tmp/somedir/myfile.zip into /tmp/somedir/

        :param filepath: Path to a local file
        :type filepath: str or unicode

        :returns: Number of extracted files
        :rtype: int
        """
        pass

    def __exit__(self, type, value, traceback):
        """
        Disconnect the ftp connection
        """
        pass

    def __enter__(self):
        """
        Establish an ftp connection and cd into the configured remote directory

        :returns: Instance of FTPHelper
        :rtype: FTPHelper
        """
        pass

    def __init__(self, remotefolder='', config=None):
        """
        Load the ftp configuration from ckan config file

        :param remotefolder: Remote folder path
        :type remotefolder: str or unicode
        """
