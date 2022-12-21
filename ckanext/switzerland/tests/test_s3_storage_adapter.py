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

ALL = {'ResponseMetadata': {'RequestId': 'KKHHB7HAEM1197ST', 'HostId': '2zVprN73/6LBDtQ3mfgG+JcWE1IMzSijOoG0JpjVi0XSz72FTD5qr+ojQgFMtZRlasAoQ34e6+0=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '2zVprN73/6LBDtQ3mfgG+JcWE1IMzSijOoG0JpjVi0XSz72FTD5qr+ojQgFMtZRlasAoQ34e6+0=', 'x-amz-request-id': 'KKHHB7HAEM1197ST', 'date': 'Wed, 21 Dec 2022 14:49:33 GMT', 'x-amz-bucket-region': 'eu-central-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 'Marker': '', 'Contents': [{'Key': 'a/', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 31, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/file_03.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'Size': 418809, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/file_04.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'Size': 418809, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/sub_a/', 'LastModified': datetime.datetime(2022, 12, 21, 13, 54, 4, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/sub_a/file_07.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()), 'ETag': '"d4f62b395733ab379de0075f209b5aef"', 'Size': 461428, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/sub_a/file_08.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()), 'ETag': '"d4f62b395733ab379de0075f209b5aef"', 'Size': 461428, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'file_01.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 52, tzinfo=tzutc()), 'ETag': '"d5100e495ad9e4587faf8f9663677584"', 'Size': 659119, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'file_02.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 53, tzinfo=tzutc()), 'ETag': '"d5100e495ad9e4587faf8f9663677584"', 'Size': 659119, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'z/', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 42, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'z/file_05.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 49, tzinfo=tzutc()), 'ETag': '"1f11c2cb8739d05738c1c08a111a93e5"', 'Size': 860443, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'z/file_06.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 49, tzinfo=tzutc()), 'ETag': '"1f11c2cb8739d05738c1c08a111a93e5"', 'Size': 860443, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}], 'Name': 'bpy-odp-test', 'Prefix': '', 'MaxKeys': 1000, 'EncodingType': 'url'}

ALL_AT_FOLDER = {'ResponseMetadata': {'RequestId': 'BMM2NE68YHB4G9GW', 'HostId': 'bGQnwz9QCSxyoI+Nr38euRaKCptuehZlbQLj4K06AgM7biYgB1mSzs75MC70kgLaEjGyLtEjaNI=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'bGQnwz9QCSxyoI+Nr38euRaKCptuehZlbQLj4K06AgM7biYgB1mSzs75MC70kgLaEjGyLtEjaNI=', 'x-amz-request-id': 'BMM2NE68YHB4G9GW', 'date': 'Wed, 21 Dec 2022 14:52:58 GMT', 'x-amz-bucket-region': 'eu-central-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}, 'IsTruncated': False, 'Marker': '', 'Contents': [{'Key': 'a/', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 31, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/file_03.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'Size': 418809, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/file_04.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'Size': 418809, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/sub_a/', 'LastModified': datetime.datetime(2022, 12, 21, 13, 54, 4, tzinfo=tzutc()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/sub_a/file_07.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()), 'ETag': '"d4f62b395733ab379de0075f209b5aef"', 'Size': 461428, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}, {'Key': 'a/sub_a/file_08.pdf', 'LastModified': datetime.datetime(2022, 12, 21, 13, 54, 22, tzinfo=tzutc()), 'ETag': '"d4f62b395733ab379de0075f209b5aef"', 'Size': 461428, 'StorageClass': 'STANDARD', 'Owner': {'ID': 'eaef31b5be35361297b84855f0bfd7e90e6e79d76430d538cdd1264ce37d3954'}}], 'Name': 'bpy-odp-test', 'Prefix': 'a/', 'MaxKeys': 1000, 'EncodingType': 'url'}

NO_CONTENT = {
}

HEAD_FILE_AT_FOLDER = {'ResponseMetadata': {'RequestId': 'N1BMBN9RRV02KCP0', 'HostId': 'Dt0lapFpP1rPL3Z9M9BdGj10IWkI8iTFBQci0bvMosJi6YaZUTkJmHeFrdANZTkx/UMIQLh8M2m1n0BzZP/2BA==', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'Dt0lapFpP1rPL3Z9M9BdGj10IWkI8iTFBQci0bvMosJi6YaZUTkJmHeFrdANZTkx/UMIQLh8M2m1n0BzZP/2BA==', 'x-amz-request-id': 'N1BMBN9RRV02KCP0', 'date': 'Wed, 21 Dec 2022 15:32:54 GMT', 'last-modified': 'Wed, 21 Dec 2022 13:53:08 GMT', 'etag': '"0b6858a853073a7e5a3edb54a51154b1"', 'accept-ranges': 'bytes', 'content-type': 'application/pdf', 'server': 'AmazonS3', 'content-length': '418809'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc()), 'ContentLength': 418809, 'ETag': '"0b6858a853073a7e5a3edb54a51154b1"', 'ContentType': 'application/pdf', 'Metadata': {}}

HEAD_FILE_AT_ROOT = {'ResponseMetadata': {'RequestId': '2ZYCRJQ7XK8RVGPE', 'HostId': '5+CArigCaxO03lAWRezv5YIRXlFYtfdsuYjtNXOSpAOiNvnMUv/aO//18M4vjA4bPy7QGoNhxF4=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '5+CArigCaxO03lAWRezv5YIRXlFYtfdsuYjtNXOSpAOiNvnMUv/aO//18M4vjA4bPy7QGoNhxF4=', 'x-amz-request-id': '2ZYCRJQ7XK8RVGPE', 'date': 'Wed, 21 Dec 2022 15:33:53 GMT', 'last-modified': 'Wed, 21 Dec 2022 13:52:52 GMT', 'etag': '"d5100e495ad9e4587faf8f9663677584"', 'accept-ranges': 'bytes', 'content-type': 'application/pdf', 'server': 'AmazonS3', 'content-length': '659119'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2022, 12, 21, 13, 52, 52, tzinfo=tzutc()), 'ContentLength': 659119, 'ETag': '"d5100e495ad9e4587faf8f9663677584"', 'ContentType': 'application/pdf', 'Metadata': {}}


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
        storage_adapter.cdremote('/foo/')

        self.assertEqual('foo', storage_adapter._working_directory)
    
    def test_cdremote_twice_then_working_directory_is_stored(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('foo/')
        storage_adapter.cdremote('bar')

        self.assertEqual('foo/bar', storage_adapter._working_directory)
    
    def test_cdremote_slash_then_working_directory_is_root(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('foo/')
        storage_adapter.cdremote('/')

        self.assertEqual('', storage_adapter._working_directory)
    
    def test_cdremote_with_slashes_then_working_directory_is_root(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('foo/bar')

        self.assertEqual('foo/bar', storage_adapter._working_directory)

    def test_cdremote_empty_then_working_directory_is_unchanged(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('foo/')
        storage_adapter.cdremote('')

        self.assertEqual('foo', storage_adapter._working_directory)

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
    
    def test_get_remote_filelist_with_folder_then_returns_the_correct_names(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
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

        files_list = storage_adapter.get_remote_filelist(folder='a')

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
    
    def test_get_remote_dirlist_at_folder_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist()

        expected_dir_list = [
            "file_03.pdf",
            "file_04.pdf",
            "sub_a/"
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_remote_dirlist_with_folder_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist('a')

        expected_dir_list = [
            "file_03.pdf",
            "file_04.pdf",
            "sub_a/"
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_remote_dirlist_all_when_no_dir_then_returns_empty_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", NO_CONTENT, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '',
                                'Prefix': ''
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist_all()

        assert_array_equal([], dir_list)

    def test_get_remote_dirlist_all_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", ALL, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '',
                                'Prefix': ''
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist_all()

        expected_dir_list = [
            "a/",
            "a/file_03.pdf",
            "a/file_04.pdf",
            "a/sub_a/",
            "a/sub_a/file_07.pdf",
            "a/sub_a/file_08.pdf",
            "file_01.pdf",
            "file_02.pdf",
            "z/",
            "z/file_05.pdf",
            "z/file_06.pdf"
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_remote_dirlist_all_at_folder_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", ALL_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '',
                                'Prefix': 'a/'
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist_all()

        expected_dir_list = [
            "file_03.pdf",
            "file_04.pdf",
            "sub_a/",
            "sub_a/file_07.pdf",
            "sub_a/file_08.pdf",
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_remote_dirlist_all_with_folder_then_returns_correct_list(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", ALL_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Delimiter': '',
                                'Prefix': 'a/'
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist_all('a')

        expected_dir_list = [
            "file_03.pdf",
            "file_04.pdf",
            "sub_a/",
            "sub_a/file_07.pdf",
            "sub_a/file_08.pdf",
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_modified_date_file_at_root_then_date_is_correct(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("head_object", HEAD_FILE_AT_ROOT, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Key': 'file_01.pdf'
                            })
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf")

        expected_date = datetime.datetime(2022,12,21,13,52,52, tzinfo=tzutc())
        assert_array_equal(expected_date, last_modified_date)
    
    def test_get_modified_date_file_at_folder_then_date_is_correct(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("head_object", HEAD_FILE_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Key': 'a/file_01.pdf'
                            })
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf")

        expected_date = datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc())
        self.assertEqual(expected_date, last_modified_date)
    
    def test_get_modified_date_file_with_folder_then_date_is_correct(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("head_object", HEAD_FILE_AT_FOLDER, {
                                'Bucket': AWS_BUCKET_NAME, 
                                'Key': 'a/file_01.pdf'
                            })
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf", 'a')

        expected_date = datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc())
        self.assertEqual(expected_date, last_modified_date)
    
    def test_get_modified_date_non_existing_file_then_date_is_correct(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_client_error('head_object')
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf", 'a')

        self.assertIsNone(last_modified_date)