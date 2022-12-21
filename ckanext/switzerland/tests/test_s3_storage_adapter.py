import unittest
import os
import shutil

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.s3_storage_adapter import S3StorageAdapter
# -----------------------------------------------------------------------

class TestS3StorageAdapter(unittest.TestCase):
    temp_folder = '/tmp/s3harvest/tests/'
    remote_folder = '/tests'
    config = {}

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

        self.assertEqual(self.config, storage_adapter.config)
    
    def test_init_without_config_then_exception_is_raised(self):
        self.failUnlessRaises(Exception, S3StorageAdapter, None, self.remote_folder)

