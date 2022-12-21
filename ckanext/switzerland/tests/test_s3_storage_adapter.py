import unittest
import os
import shutil

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.s3_storage_adapter import S3StorageAdapter
# -----------------------------------------------------------------------

class TestS3StorageAdapter(unittest.TestCase):
    temp_folder = '/tmp/s3harvest/tests/'

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
    
    def test_init_when_remote_folder_then_stored(self):
        remote_folder = '/test/'

        storage_adapter = S3StorageAdapter(remote_folder)

        self.assertEqual('/test', storage_adapter.remote_folder)
    
    def test_init_without_remote_folder_then_empty(self):
        config = {}

        storage_adapter = S3StorageAdapter(config=config)

        self.assertEqual('', storage_adapter.remote_folder)

    def test_init_when_config_then_stored(self):
        config = {}

        storage_adapter = S3StorageAdapter(config=config)

        self.assertEqual(config, storage_adapter.config)
    
    def test_init_without_config_then_exception_is_raised(self):
        remote_folder = '/test/'

        self.assertRaises(Exception, S3StorageAdapter(remote_folder))

