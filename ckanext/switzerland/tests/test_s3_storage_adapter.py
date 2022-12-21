import unittest
import os
import shutil

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.s3_storage_adapter import S3StorageAdapter
# -----------------------------------------------------------------------

from ckanext.switzerland.harvester.aws_keys import AWS_SECRET_KEY, AWS_ACCESS_KEY, AWS_REGION_NAME

LOCAL_PATH='localpath'

class TestS3StorageAdapter(unittest.TestCase):
    temp_folder = '/tmp/s3harvest/tests/'
    remote_folder = '/tests'
    config = {
        LOCAL_PATH: temp_folder, 
        AWS_SECRET_KEY: "secret_key",
        AWS_ACCESS_KEY: "access_key",
        AWS_REGION_NAME: "region_name"
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
        # remove the tmp directory
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder, ignore_errors=True)
    
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
        self.failUnlessRaises(Exception, S3StorageAdapter, None, self.remote_folder)

    def test_init_then_temp_folder_is_created(self):
        folder = self.config[LOCAL_PATH]
        if os.path.exists(folder):
            os.rmdir(folder)

        S3StorageAdapter(self.config, self.remote_folder)

        assert os.path.exists(folder)

    def test_connect_then_client_is_initialized(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter._connect()

        self.assertIsNotNone(storage_adapter._aws_session)

    def test_cdremote_then_working_directory_is_stored(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        remote_dir = '/remote/'
        storage_adapter.cdremote(remote_dir)

        self.assertEqual('/remote', storage_adapter._working_directory)

    def test_cdremote_when_remotedir_is_none_then_working_directory_is_correct(self):
        storage_adapter = S3StorageAdapter(self.config, self.remote_folder)
        storage_adapter.cdremote(None)

        self.assertEqual('/', storage_adapter._working_directory)
        

    def test_with_syntax_then_working_session_is_created(self):
        with S3StorageAdapter(self.config, self.remote_folder) as storage_adapter:
            self.assertEqual('/', storage_adapter._working_directory)
