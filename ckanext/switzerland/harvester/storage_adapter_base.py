import errno
import logging
import os
import zipfile
from pprint import pformat

from ckanext.switzerland.harvester.exceptions.storage_adapter_configuration_exception import (
    StorageAdapterConfigurationException,
)
from ckanext.switzerland.harvester.keys import LOCAL_PATH

log = logging.getLogger(__name__)


class StorageAdapterBase(object):
    _config = None
    _ckan_config_resolver = None
    remote_folder = None
    _config_keys = []

    def __init__(
        self,
        ckan_config_resolver,
        config,
        remote_folder="",
        root_config_key=None,
        config_keys=[],
        config_key_prefix="",
    ):
        """
        Load the ftp configuration from ckan config file

        :param ckan_config_resolver: An object able to read the CKAN config file. Injected for testing purposes
        :type ckan_config_resolver: ckan.plugins.toolkit.config
        :param config: The harvester config coming from the database.
        :type config: Any
        :param remotefolder: Remote folder path. Can be different that the one stored in the harvester config
        :type remotefolder: str or unicode
        :param root_config_key: The property in the configuration (from database) that indicates how to find the StorageAdapter configuration in the CKAN configuration file
        :type root_config_key: str
        :param config_keys: An array of ConfigKey, describing all the configuration properties needed, and their constraints
        :type config_keys: Array of ConfigKey
        :param config_key_prefix: A string representing the prefix to use to find the configuration keys in the CKAN configuration file
        :type config_key_prefix: str

        """

        # Validate the basic config. We need the config, and we need to know what is the root key.
        if config is None:
            raise StorageAdapterConfigurationException(
                ["Cannot build a Storage Adapter without an initial configuration"]
            )

        if root_config_key is None:
            raise StorageAdapterConfigurationException(
                ["Cannot build a Storage Adapter without an root config key"]
            )

        if root_config_key not in config:
            raise StorageAdapterConfigurationException(
                [
                    "The root config key '{key}' is not present in the configuration".format(
                        key=root_config_key
                    )
                ]
            )

        # Just store all the parameters
        self._config = config
        self._config_keys = config_keys
        self._ckan_config_resolver = ckan_config_resolver
        self.remote_folder = remote_folder.rstrip("/")

        # Compute the prefix (eg: ckan.ftp.main_server, ckan.s3.main_bucket).
        # Config keys that are defined as env vars cannot have - in them: _ is
        # used instead. If S3 bucket names contain -, replace it with _ to get
        # the config key.
        config_key = self._config[root_config_key].replace("-", "_")
        config_key_prefix = "{key_prefix}.{key}".format(
            key_prefix=config_key_prefix,
            key=config_key
        )
        # Load and validate the config at the same time
        self.__load_storage_config__(config_key_prefix)

        self.create_local_dir()

        log.debug('Using Config: %s' % pformat(self._config))

    def _connect(self):
        """
        Create a connection to the storage if needed.
        """
        raise NotImplementedError("_connect")

    def _disconnect(self):
        """
        Closes the connection if needed.
        """
        raise NotImplementedError("_disconnect")

    # tested
    def __enter__(self):
        """
        Method called from the 'with' syntax.
        Do there whatever is needed after instancing the StorageAdapter
        """
        raise NotImplementedError("__enter__")

    # tested
    def __exit__(self, type, value, traceback):
        """
        Method called from the 'with' syntax.
        Do here whatever is needed to clean the StorageAdapter before it is destroyed.
        """
        raise NotImplementedError("__exit__")

    def get_top_folder(self):
        """
        Get the name of the top-most folder in /tmp

        :returns: The name of the local folder created by the StorageAdapter
        :rtype: string
        """
        raise NotImplementedError("get_top_folder")

    def create_local_dir(self, folder=None):
        """
        Create a local folder

        :param folder: Folder path
        :type folder: str or unicode

        :returns: None
        :rtype: None
        """
        if not folder:
            folder = self._config["localpath"]
        # create the local directory if it does not exist

        folder = folder.rstrip("/")

        if not os.path.isdir(folder):
            self._mkdir_p(folder)
            log.debug("Created folder: %s" % str(folder))

    # tested
    def _mkdir_p(self, path, perms=0o777):
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
        raise NotImplementedError("cdremote")

    def get_remote_filelist(self, folder=None):
        """
        List files in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        raise NotImplementedError("get_remote_filelist")

    def get_remote_dirlist(self, folder=None):
        """
        List files and sub-directories in the current directory

        :param folder: Full path on the remote server
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        raise NotImplementedError("get_remote_dirlist")

    def get_remote_dirlist_all(self, folder=None):
        """
        Get a listing of all files (including subdirectories in a specific folder on the remote server

        :param folder: Folder name or path
        :type folder: str or unicode

        :returns: Directory listing (excluding '.' and '..')
        :rtype: list
        """
        raise NotImplementedError("get_remote_dirlist_all")

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

        :returns: The date at which the file has been last modified
        :rtype: Datetime
        """
        raise NotImplementedError("get_modified_date")

    def get_local_path(self):
        return self._config[LOCAL_PATH]

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
        Fetch a single file from the remote server

        :param filename: File to fetch
        :type filename: str or unicode
        :param localpath: Local folder to store the file
        :type localpath: str or unicode

        :returns: Status of the operation
        :rtype: string
        """
        raise NotImplementedError("fetch")

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
        if file_extension.lower() == ".zip":
            log.info("Unzipping: %s" % filepath)
            target_folder = os.path.dirname(filepath)
            zfile = zipfile.ZipFile(filepath)
            filelist = zfile.namelist()
            zfile.extractall(target_folder)
            return len(filelist)

    # tested in TestS3StorageAdapter and TestFTPStorageAdapter
    def __load_storage_config__(self, key_prefix=""):
        """
        This method will load and validate the configuration

        For each config_key in the array config_keys, this method will
            - Read the raw value from the CKAN configuration file, using the prefix to create the correct name (eg: ckan.ftp.main_server.host)
            - If the config key is marked as mandatory, it will validate that there is a value, raise an error otherwise
            - Try to convert the value to the required type, raise an error otherwise
            - Validate constraints, if exists, on the value (eg: x > 0), raise an error otherwise.
            - Store the converted value in the config object if none of the above raised an error
        """

        configuration_errors = []
        for config_key in self._config_keys:
            raw_value = self._ckan_config_resolver.get(
                key_prefix + ".%s" % config_key.name, ""
            )

            if config_key.is_mandatory and (raw_value is None or len(raw_value) == 0):
                configuration_errors.append(
                    "Configuration is missing the field {key}".format(
                        key=config_key.name
                    )
                )
                continue

            converted_value = None

            try:
                converted_value = config_key.type(raw_value)

            except ValueError:
                configuration_errors.append(
                    "Cannot convert '{value}' for the field '{key}' into type {type}".format(
                        value=raw_value, key=config_key.name, type=config_key.type
                    )
                )
                continue

            if not config_key.is_valid(converted_value):
                if config_key.custom_error_message is not None:
                    configuration_errors.append(config_key.custom_error_message)
                else:
                    configuration_errors.append(
                        "The value '{value}' does not match the constraints for the field '{key}'".format(
                            value=raw_value, key=config_key.name
                        )
                    )
                continue

            # Temporary workaround for passwords that contain %: we have to
            # escape it as %% in .env, but the extra % is not removed by Docker.
            # Todo: remove this as soon as we have a new password.
            if isinstance(converted_value, str):
                converted_value = converted_value.replace("%%", "%")

            self._config[config_key.name] = converted_value

        if len(configuration_errors) > 0:
            raise StorageAdapterConfigurationException(configuration_errors)
