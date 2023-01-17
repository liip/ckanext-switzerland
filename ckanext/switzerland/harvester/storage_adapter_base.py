import os
import logging
import errno
import zipfile

log = logging.getLogger(__name__)
#TODO: Rewrite documentation
class StorageAdapterBase(object):
    _config = None
    _ckan_config_resolver= None
    remote_folder = None

    def __init__(self, ckan_config_resolver, config, remote_folder=''):
        """
        Load the ftp configuration from ckan config file

        :param remotefolder: Remote folder path
        :type remotefolder: str or unicode
        :param ckan_config_resolver: The object able to provide values from CKAN ini configuration
        :type ckan_config_resolver: ckan.plugins.toolkit.config
        :param config: The harvester config coming from the database
        :type config: Any
        """
        if config is None:
            raise Exception('Cannot build a Storage Adapter without an initial configuration')
        
        #TODO: Call here an abstract method, that would validate the configuration and throw when invalid
        # with an abstract method, we force the adapter to validate itself its configuration, depending on the needs
        self._config = config

        self._ckan_config_resolver = ckan_config_resolver
        
        self.remote_folder = remote_folder.rstrip("/")


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
        raise NotImplementedError('cdremote')

    def get_remote_filelist(self, folder=None):
        """
        List files in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        raise NotImplementedError('get_remote_filelist')

    def get_remote_dirlist(self, folder=None):
        """
        List files and sub-directories in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        raise NotImplementedError('get_remote_dirlist')

    def get_remote_dirlist_all(self, folder=None):
        """
        Get a listing of all files (including subdirectories in a specific folder on the remote server

        :param folder: Folder name or path
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        raise NotImplementedError('get_remote_dirlist_all')

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
        raise NotImplementedError('get_modified_date')

    def get_local_path(self):
        return self._config['localpath']


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
        raise NotImplementedError('fetch')

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

    #tested in TestS3StorageAdapter
    def __load_storage_config__(self, keys, key_prefix=""):
        for key in keys:
            self._config[key] = self._ckan_config_resolver.get(key_prefix+'.%s' % key, '')
