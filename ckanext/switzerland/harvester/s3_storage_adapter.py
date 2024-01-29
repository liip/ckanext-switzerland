"""
S3 Storage Adapter
==================

Methods that help with dealing with remote AWS S3 Storage and local folders.
The class is intended to be used with Python's `with` statement, e.g.
`
    with S3StorageAdapter('/remote-base-path/', config, ...) as storage:
        ...
`
"""

import datetime
import logging
import os
import re

import boto3
import boto3.session
from botocore.exceptions import ClientError
from dateutil.tz import tzutc

from ckanext.switzerland.harvester.config.config_key import ConfigKey
from ckanext.switzerland.harvester.keys import (
    AWS_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION_NAME,
    AWS_RESPONSE_CONTENT,
    AWS_RESPONSE_PREFIXES,
    AWS_SECRET_KEY,
    LOCAL_PATH,
    REMOTE_DIRECTORY,
    S3_CONFIG_KEY,
)
from ckanext.switzerland.harvester.storage_adapter_base import StorageAdapterBase

log = logging.getLogger(__name__)

CONFIG_KEYS = [
    ConfigKey(AWS_BUCKET_NAME, str, True),
    ConfigKey(AWS_ACCESS_KEY, str, True),
    ConfigKey(AWS_REGION_NAME, str, True),
    ConfigKey(AWS_SECRET_KEY, str, True),
    ConfigKey(LOCAL_PATH, str, True),
    ConfigKey(REMOTE_DIRECTORY, str, True),
]


class S3StorageAdapter(StorageAdapterBase):
    _aws_session = None
    _aws_client = None
    _working_directory = ""

    def __init__(self, config_resolver, config, remote_folder=""):
        super(S3StorageAdapter, self).__init__(
            config_resolver,
            config,
            remote_folder,
            S3_CONFIG_KEY,
            CONFIG_KEYS,
            "ckan.s3",
        )

    def __enter__(self):
        self._connect()
        self.cdremote(self.remote_folder)
        return self

    def __exit__(self, type, value, traceback):
        pass

    def _connect(self):
        self._aws_session = boto3.session.Session(
            aws_access_key_id=self._config[AWS_ACCESS_KEY],
            aws_secret_access_key=self._config[AWS_SECRET_KEY],
            region_name=self._config[AWS_REGION_NAME],
        )

        self._aws_client = self._aws_session.client("s3")

    def _disconnect(self):
        # as boto3 is HTTP call based, we don't need to close anything
        pass

    def cdremote(self, remotedir=None):
        # Files are stored flat on AWS. So there is no such command on S3. We just need
        # to keep a ref to a Working Directory
        if remotedir == "/":
            self._working_directory = ""
        elif remotedir:
            self._working_directory = os.path.join(
                self._working_directory, remotedir.rstrip("/").lstrip("/")
            )

    def get_top_folder(self):
        # the top folder is basically just the name of the bucket.
        return self._config[AWS_BUCKET_NAME]

    def get_remote_filelist(self, folder=None):
        # get list of the files in the remote folder
        all_in_folder = self.get_remote_dirlist(folder)
        only_files = [name for name in all_in_folder if not name.endswith("/")]
        return only_files

    def __remove_prefix__(self, file, prefix):
        if not file.startswith(prefix):
            return file

        if prefix is None or len(prefix) == 0:
            return file

        return file[len(prefix) :]

    def __prepare_for_return__(self, elements, prefix):
        # AWS returns the element with their full name from root, so we need to remove
        # the prefix
        without_prefix = [self.__remove_prefix__(file, prefix) for file in elements]
        # Of course, we will now have a empty string in the set, let's remove it
        without_root = [name for name in without_prefix if name]
        return without_root

    def __determine_prefix__(self, folder):
        prefix = folder if folder is not None else self._working_directory
        prefix = prefix + "/" if prefix else ""
        return prefix

    def __clean_aws_response__(self, s3_objects):
        if not s3_objects or AWS_RESPONSE_CONTENT not in s3_objects:
            return []

        return [object["Key"] for object in s3_objects[AWS_RESPONSE_CONTENT]]

    def get_remote_dirlist(self, folder=None):
        prefix = self.__determine_prefix__(folder)

        # By fixing the delimiter to '/', we limit the results to the current folder
        s3_objects = self._aws_client.list_objects(
            Bucket=self._config[AWS_BUCKET_NAME], Prefix=prefix, Delimiter="/"
        )

        objects = self.__clean_aws_response__(s3_objects)

        # But the previous call, did not return the folders (because of setting a
        # delimiter), so lets look in the prefixes to add them
        if AWS_RESPONSE_PREFIXES in s3_objects:
            objects.extend(
                [object["Prefix"] for object in s3_objects[AWS_RESPONSE_PREFIXES]]
            )

        # AWS always returns sorted items. Usually no need to sort. In this case we need
        # to sort as we aggregated two sources
        files_and_folder = sorted(self.__prepare_for_return__(objects, prefix))

        return files_and_folder

    def get_remote_dirlist_all(self, folder=None):
        prefix = self.__determine_prefix__(folder)

        # By fixing the delimiter to '', we list full depth, starting at the prefix
        # depth
        s3_objects = self._aws_client.list_objects(
            Bucket=self._config[AWS_BUCKET_NAME], Prefix=prefix, Delimiter=""
        )

        objects = self.__clean_aws_response__(s3_objects)

        return self.__prepare_for_return__(objects, prefix)

    def get_modified_date(self, filename, folder=None):
        prefix = self.__determine_prefix__(folder)
        file_full_path = os.path.join(prefix, filename)
        try:
            s3_object = self._aws_client.head_object(
                Bucket=self._config[AWS_BUCKET_NAME], Key=file_full_path
            )
            if (
                s3_object["LastModified"].tzinfo is not None
                and s3_object["LastModified"].tzinfo == tzutc()
            ):
                modified_date = str(s3_object["LastModified"])
                modified_date = datetime.datetime.strptime(
                    modified_date[:-6], "%Y-%m-%d %H:%M:%S"
                )
                # example: 2022-11-02 13:46:07
                return modified_date
            else:
                log.info(
                    "S3 bucket modified date information is not available "
                    "or timezone is not in UTC"
                )
        except ClientError:
            return None

    def fetch(self, filename, localpath=None):
        prefix = self.__determine_prefix__(None)
        file_full_path = os.path.join(prefix, filename)

        if not localpath:
            localpath = os.path.join(self._config[LOCAL_PATH], filename)

        # ensure that the local path is a valid director
        local_tmp_path = re.match(r"(.*)/[^/]+$", localpath).group(1)
        if not os.path.exists(local_tmp_path):
            os.makedirs(local_tmp_path)

        self._aws_client.download_file(
            self._config[AWS_BUCKET_NAME], file_full_path, localpath
        )

        return "226 Transfer complete"
