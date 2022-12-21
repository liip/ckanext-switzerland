import unittest
import os
import shutil
import datetime
import boto3
from dateutil.tz import tzutc
from botocore.stub import Stubber
from numpy.testing import assert_array_equal

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.s3_storage_adapter import S3StorageAdapter
# -----------------------------------------------------------------------

from ckanext.switzerland.harvester.aws_keys import (
    AWS_SECRET_KEY,
    AWS_ACCESS_KEY,
    AWS_REGION_NAME,
    AWS_BUCKET_NAME
)

LOCAL_PATH = 'localpath'

FILES_AT_ROOT = {'ResponseMetadata': {'RequestId': '8XVDSP6S2ZR26YE9', 'HostId': 'XZ0TbGFfbC2q+T9gm27jyG8jkyzWV4oJZmKtheUZwNPD0c5/HoZnasKalnaA6Z200HDa0lJIPYQ=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'XZ0TbGFfbC2q+T9gm27jyG8jkyzWV4oJZmKtheUZwNPD0c5/HoZnasKalnaA6Z200HDa0lJIPYQ=', 'x-amz-request-id': '8XVDSP6S2ZR26YE9', 'date': 'Wed, 21 Dec 2022 14:30:47 GMT', 'x-amz-bucket-region': 'eu-central-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 'Marker': '', 'Contents': [{'Key': 'file_01.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 52, tzinfo=tzutc()), 'ETag': '"d5100e495ad9e4587faf8f9663677584"', 'Size': 659119, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'file_02.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 53, tzinfo=tzutc()), 'ETag': '"d5100e495ad9e4587faf8f9663677584"', 'Size': 659119, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}], 'Name': 'bpy-odp-test', 'Prefix': '', 'Delimiter': '/', 'MaxKeys': 1000, 'CommonPrefixes': [{'Prefix': 'a/'}, {'Prefix': 'z/'}], 'EncodingType': 'url'}

FILES_AT_FOLDER = {'ResponseMetadata': {'RequestId': 'WR5RWMG0ANK7HBJP', 'HostId': 'RqOynGjyaE0GaP1h1TMWLYt2TFlV0AaNduBt2dGKqZpz0vgKG3lVV1rv+i4g0n6qlp9cIo/CshY=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'RqOynGjyaE0GaP1h1TMWLYt2TFlV0AaNduBt2dGKqZpz0vgKG3lVV1rv+i4g0n6qlp9cIo/CshY=', 'x-amz-request-id': 'WR5RWMG0ANK7HBJP', 'date': 'Wed, 21 Dec 2022 14:30:01 GMT', 'x-amz-bucket-region': 'eu-central-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 'Marker': '', 'Contents': [{'Key': 'a/', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 31, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/file_03.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'Size': 418809, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/file_04.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'Size': 418809, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}], 'Name': 'bpy-odp-test', 'Prefix': 'a/', 'Delimiter': '/', 'MaxKeys': 1000, 'CommonPrefixes': [{'Prefix': 'a/sub_a/'}], 'EncodingType': 'url'}

NO_CONTENT = {
}


class TestS3StorageAdapter(unittest.TestCase):
    temp_folder = '/tmp/s3harvest/tests/'
    remote_folder = '/tests'
    config = {
        LOCAL_PATH: temp_folder,
        AWS_SECRET_KEY: "secret_key",
        AWS_ACCESS_KEY: "access_key",
        AWS_REGION_NAME: "eu-central-1",
        AWS_BUCKET_NAME: AWS_BUCKET_NAME
    }

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setup(self):
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder, ignore_errors=True)

    def teardown(self):
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder, ignore_errors=True)

    def __stub_aws_client__(self, storage_adapter):
        client = boto3.client('s3')
        storage_adapter._aws_client = client
        stubber = Stubber(client)
        return stubber

    def test_init_when_remote_folder_then_stored_without_trailing_slash(self):
        remote_folder = '/test/'

        storage_adapter = S3StorageAdapter(self.config, remote_folder)

        self.assertEqual('/test', storage_adapter.remote_folder)

    def test_init_without_remote_folder_then_empty(self):
        storage_adapter = S3StorageAdapter(config=self.config)

        self.assertEqual('', storage_adapter.remote_folder)

    def test_init_when_config_then_stored(self):
        storage_adapter = S3StorageAdapter(config=self.config)

        self.assertEqual(self.config, storage_adapter._config)

    def test_init_without_config_then_exception_is_raised(self):
        self.failUnlessRaises(Exception, S3StorageAdapter,
                              None, self.remote_folder)

    def test_init_then_temp_folder_is_created(self):
        folder = self.config[LOCAL_PATH]
        if os.path.exists(folder):
            os.rmdir(folder)

        S3StorageAdapter(self.config, self.remote_folder)

        assert os.path.exists(folder)

    def test_connect_then_session_is_initialized(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter._connect()

        self.assertIsNotNone(storage_adapter._aws_session)

    def test_connect_then_client_is_initialized(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter._connect()

        self.assertIsNotNone(storage_adapter._aws_client)

    def test_cdremote_then_working_directory_is_stored(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        remote_dir = '/remote/'
        storage_adapter.cdremote(remote_dir)

        self.assertEqual('remote', storage_adapter._working_directory)

    def test_cdremote_when_remotedir_is_none_then_working_directory_is_correct(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote(None)

        self.assertEqual('', storage_adapter._working_directory)

    def test_with_syntax_then_working_session_is_created(self):
        with S3StorageAdapter(self.config, self.remote_folder) as storage_adapter:
            self.assertEqual('', storage_adapter._working_directory)

    def test_get_top_folder_then_returns_bucket_name(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)

        self.assertEqual(self.config[AWS_BUCKET_NAME],
                         storage_adapter.get_top_folder())

    def test_get_remote_filelist_at_root_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_ROOT, {
                                'Bucket': AWS_BUCKET_NAME,
                                'Delimiter': '/',
                                'Prefix': ''
                            })
        stubber.activate()
        expected_files_list = [
            "file_01.pdf",
            "file_02.pdf"
        ]

        files_list = storage_adapter.get_remote_filelist()

        assert_array_equal(expected_files_list, files_list)

    def test_get_remote_filelist_at_folder_then_returns_the_correct_names(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()
        expected_files_list = [
            "file_03.pdf",
            "file_04.pdf"
        ]

        files_list = storage_adapter.get_remote_filelist()

        assert_array_equal(expected_files_list, files_list)

    def test_get_remote_filelist_at_empty_folder_then_returns_empty_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('empty')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", NO_CONTENT, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'empty/'
                            })
        stubber.activate()
        files_list = storage_adapter.get_remote_filelist()

        assert_array_equal([], files_list)

    def test_get_remote_dirlist_when_no_dir_then_returns_empty_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", NO_CONTENT, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': ''
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist()

        assert_array_equal([], dir_list)

    def test_get_remote_dirlist_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_ROOT, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': ''
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist()

        expected_dir_list = [
            "a/",
            "file_01.pdf",
            "file_02.pdf",
            "z/"
        ]
        assert_array_equal(expected_dir_list, dir_list)

