import unittest
import os
import shutil
import datetime

from helpers.mock_config_resolver import MockConfigResolver
from fixtures.aws_fixture import FILES_AT_ROOT, FILE_CONTENT, FILES_AT_FOLDER, HEAD_FILE_AT_FOLDER, HEAD_FILE_AT_ROOT, NO_CONTENT, ALL, ALL_AT_FOLDER, GET_OBJECT
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
CONFIG_SECTION = 'app:main'
CONFIG_BUCKET = 'bucket'
TEST_BUCKET_NAME = 'test-bucket'
class TestS3StorageAdapter(unittest.TestCase):
    temp_folder = '/tmp/s3harvest/tests/'
    ini_file_path = './ckanext/switzerland/tests/config/nosetest.ini'
    remote_folder = 'a'
    config = {
        LOCAL_PATH: temp_folder,
        CONFIG_BUCKET: 'main_bucket'
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

    def __build_tested_object__(self):
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        return S3StorageAdapter(config_resolver, self.config, self.remote_folder)

    def __stub_aws_client__(self, storage_adapter):
        client = boto3.client('s3')
        storage_adapter._aws_client = client
        stubber = Stubber(client)
        return stubber

    def test_init_when_remote_folder_then_stored_without_trailing_slash(self):
        self.remote_folder = '/test/'

        storage_adapter = self.__build_tested_object__()

        self.assertEqual('/test', storage_adapter.remote_folder)

    def test_init_without_remote_folder_then_empty(self):
        self.remote_folder = ''
        storage_adapter = self.__build_tested_object__()

        self.assertEqual('', storage_adapter.remote_folder)

    def test_init_when_config_then_stored(self):
        self.remote_folder = ''
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(self.config, storage_adapter._config)

    def test_init_without_config_then_exception_is_raised(self):
        self.failUnlessRaises(Exception, S3StorageAdapter,
                              None, self.remote_folder)

    def test_init_then_temp_folder_is_created(self):
        folder = self.config[LOCAL_PATH]
        if os.path.exists(folder):
            shutil.rmtree(folder)

        self.__build_tested_object__()

        assert os.path.exists(folder)

    def test_connect_then_session_is_initialized(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter._connect()

        self.assertIsNotNone(storage_adapter._aws_session)

    def test_connect_then_client_is_initialized(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter._connect()

        self.assertIsNotNone(storage_adapter._aws_client)

    def test_cdremote_then_working_directory_is_stored(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('/foo/')

        self.assertEqual('foo', storage_adapter._working_directory)
    
    def test_cdremote_twice_then_working_directory_is_stored(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('foo/')
        storage_adapter.cdremote('bar')

        self.assertEqual('foo/bar', storage_adapter._working_directory)
    
    def test_cdremote_slash_then_working_directory_is_root(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('foo/')
        storage_adapter.cdremote('/')

        self.assertEqual('', storage_adapter._working_directory)
    
    def test_cdremote_with_slashes_then_working_directory_is_root(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('foo/bar')

        self.assertEqual('foo/bar', storage_adapter._working_directory)

    def test_cdremote_empty_then_working_directory_is_unchanged(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('foo/')
        storage_adapter.cdremote('')

        self.assertEqual('foo', storage_adapter._working_directory)

    def test_cdremote_when_remotedir_is_none_then_working_directory_is_correct(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote(None)

        self.assertEqual('', storage_adapter._working_directory)

    def test_with_syntax_then_working_session_is_created(self):
        with self.__build_tested_object__() as storage_adapter:
            self.assertEqual(self.remote_folder, storage_adapter._working_directory)

    def test_get_top_folder_then_returns_bucket_name(self):
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(self.config[AWS_BUCKET_NAME],
                         storage_adapter.get_top_folder())

    def test_get_remote_filelist_at_root_then_returns_correct_list(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_ROOT, {
                                'Bucket': TEST_BUCKET_NAME,
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
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()
        expected_files_list = [
            "a_file_05.pdf",
            "file_03.pdf",
            "file_04.pdf"
        ]

        files_list = storage_adapter.get_remote_filelist()

        assert_array_equal(expected_files_list, files_list)
    
    def test_get_remote_filelist_with_folder_then_returns_the_correct_names(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()
        expected_files_list = [
            "a_file_05.pdf",
            "file_03.pdf",
            "file_04.pdf"
        ]

        files_list = storage_adapter.get_remote_filelist(folder='a')

        assert_array_equal(expected_files_list, files_list)

    def test_get_remote_filelist_at_empty_folder_then_returns_empty_list(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('empty')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", NO_CONTENT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'empty/'
                            })
        stubber.activate()
        files_list = storage_adapter.get_remote_filelist()

        assert_array_equal([], files_list)

    def test_get_remote_dirlist_when_no_dir_then_returns_empty_list(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", NO_CONTENT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': ''
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist()

        assert_array_equal([], dir_list)

    def test_get_remote_dirlist_then_returns_correct_list(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_ROOT, {
                                'Bucket': TEST_BUCKET_NAME, 
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
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist()

        expected_dir_list = [
            "a_file_05.pdf",
            "file_03.pdf",
            "file_04.pdf",
            "sub_a/"
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_remote_dirlist_with_folder_then_returns_correct_list(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", FILES_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '/',
                                'Prefix': 'a/'
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist('a')

        expected_dir_list = [
            "a_file_05.pdf",
            "file_03.pdf",
            "file_04.pdf",
            "sub_a/"
        ]
        assert_array_equal(expected_dir_list, dir_list)
    
    def test_get_remote_dirlist_all_when_no_dir_then_returns_empty_list(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", NO_CONTENT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Delimiter': '',
                                'Prefix': ''
                            })
        stubber.activate()

        dir_list = storage_adapter.get_remote_dirlist_all()

        assert_array_equal([], dir_list)

    def test_get_remote_dirlist_all_then_returns_correct_list(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", ALL, {
                                'Bucket': TEST_BUCKET_NAME, 
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
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", ALL_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
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
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("list_objects", ALL_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
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
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("head_object", HEAD_FILE_AT_ROOT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'file_01.pdf'
                            })
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf")

        expected_date = datetime.datetime(2022,12,21,13,52,52, tzinfo=tzutc())
        assert_array_equal(expected_date, last_modified_date)
    
    def test_get_modified_date_file_at_folder_then_date_is_correct(self):
        storage_adapter = self.__build_tested_object__()
        storage_adapter.cdremote('a')
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("head_object", HEAD_FILE_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'a/file_01.pdf'
                            })
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf")

        expected_date = datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc())
        self.assertEqual(expected_date, last_modified_date)
    
    def test_get_modified_date_file_with_folder_then_date_is_correct(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("head_object", HEAD_FILE_AT_FOLDER, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'a/file_01.pdf'
                            })
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf", 'a')

        expected_date = datetime.datetime(2022, 12, 21, 13, 53, 8, tzinfo=tzutc())
        self.assertEqual(expected_date, last_modified_date)
    
    def test_get_modified_date_non_existing_file_then_date_is_correct(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_client_error('head_object')
        stubber.activate()

        last_modified_date = storage_adapter.get_modified_date("file_01.pdf", 'a')

        self.assertIsNone(last_modified_date)
    
    def test_fetch_then_file_is_written_on_disk(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("get_object", GET_OBJECT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'example.csv'
                            })
        stubber.activate()
        local_file = os.path.join(self.config[LOCAL_PATH], "example.csv")

        storage_adapter.fetch("example.csv")

        assert os.path.exists(local_file)
   
    def test_fetch_then_file_content_is_correct(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("get_object", GET_OBJECT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'example.csv'
                            })
        stubber.activate()
        local_file = os.path.join(self.config[LOCAL_PATH], "example.csv")

        storage_adapter.fetch("example.csv")

        with open(local_file, 'rb') as f: 
            written_bytes = f.read()

        assert_array_equal(FILE_CONTENT, written_bytes)
   
    def test_fetch_then_status_is_correct(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("get_object", GET_OBJECT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'example.csv'
                            })
        stubber.activate()
        
        status = storage_adapter.fetch("example.csv")

        self.assertEqual("226 Transfer complete", status)
    
    def test_fetch_with_local_path_then_file_is_written_on_disk(self):
        storage_adapter = self.__build_tested_object__()
        stubber = self.__stub_aws_client__(storage_adapter)
        stubber.add_response("get_object", GET_OBJECT, {
                                'Bucket': TEST_BUCKET_NAME, 
                                'Key': 'example.csv'
                            })
        stubber.activate()
        
        local_file = os.path.join('/tmp/', "example_foo.csv")
        
        status = storage_adapter.fetch("example.csv", local_file)

        assert os.path.exists(local_file)

    def test_init_when_no_config_then_throws_exception(self):
        self.config = None
        self.assertRaises(Exception, self.__build_tested_object__)

    def test_init_config_without_bucket_then_exception_is_raised(self):
        self.config = {}

        self.assertRaises(KeyError, self.__build_tested_object__)

    
    def test_init_config_then_bucket_name_is_correct(self):
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(storage_adapter._config[AWS_BUCKET_NAME], 'test-bucket')
    
    def test_init_config_then_access_key_is_correct(self):
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(storage_adapter._config[AWS_ACCESS_KEY], 'test-access-key')
    
    def test_init_config_then_secret_key_is_correct(self):
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(storage_adapter._config[AWS_SECRET_KEY], 'test-secret-key')
    
    def test_init_config_then_region_name_is_correct(self):
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(storage_adapter._config[AWS_REGION_NAME], 'eu-central-1')

    def test_init_config_then_local_path_is_correct(self):
        storage_adapter = self.__build_tested_object__()

        self.assertEqual(storage_adapter._config[LOCAL_PATH], '/tmp/s3harvest/')