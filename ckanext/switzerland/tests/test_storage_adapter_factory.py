import unittest
import os
from ckanext.switzerland.harvester.ftp_storage_adapter import FTPStorageAdapter
from ckanext.switzerland.harvester.s3_storage_adapter import S3StorageAdapter
from helpers.mock_config_resolver import MockConfigResolver
# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.storage_adapter_factory import StorageAdapterFactory
# -----------------------------------------------------------------------

class TestStorageAdapterFactory(unittest.TestCase):
    config = {}
    remote_folder = ''
    ini_file_path = '../../../templates/ckan/development.ini'
   

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass
    
    def __build_legacy_config__(self):
        self.config = {
            "ftp_server": "mainserver",
            "environment": "Test",
            "folder": "DiDok",
            "dataset": "DiDok",
            "max_resources": 30,
            "max_revisions": 30,
            "filter_regex": ".*\\.xls",
            "resource_regex": "\\d{8}-Ist-File\\.xls",
            "ist_file": True
        }
    
    def __build_s3_config__(self):
        self.config = {
            "storage_adapter": "S3",
            "environment": "Test",
            "folder": "DiDok",
            "dataset": "DiDok",
            "max_resources": 30,
            "max_revisions": 30,
            "filter_regex": ".*\\.xls",
            "resource_regex": "\\d{8}-Ist-File\\.xls",
            "ist_file": True
        }
    
    def __build_ftp_config__(self):
        self.config = {
            "storage_adapter": "S3",
            "ftp_server": "FTP",
            "environment": "Test",
            "folder": "DiDok",
            "dataset": "DiDok",
            "max_resources": 30,
            "max_revisions": 30,
            "filter_regex": ".*\\.xls",
            "resource_regex": "\\d{8}-Ist-File\\.xls",
            "ist_file": True
        }

    def test_get_storage_adapter_when_legacy_config_then_return_ftp_adapted(self):
        self.__build_legacy_config__()
        print(os.getcwd())
        config_resolver = MockConfigResolver(self.ini_file_path, 'app:main')
        factory = StorageAdapterFactory()

        adapter = factory.get_storage_adapter(config_resolver, self.remote_folder, self.config)

        assert isinstance(adapter, FTPStorageAdapter)

    def test_get_storage_adapter_when_s3_config_then_return_s3_adapter(self):
        self.__build_s3_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, 'app:main')
        factory = StorageAdapterFactory()

        adapter = factory.get_storage_adapter(config_resolver, self.remote_folder, self.config)

        assert isinstance(adapter, S3StorageAdapter)

    def test_get_storage_adapter_when_ftp_config_then_return_ftp_adapter(self):
        self.__build_ftp_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, 'app:main')
        factory = StorageAdapterFactory()

        adapter = factory.get_storage_adapter(config_resolver, self.remote_folder, self.config)

        assert isinstance(adapter, S3StorageAdapter)


