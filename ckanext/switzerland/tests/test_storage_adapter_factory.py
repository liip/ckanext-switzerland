import unittest
import os
from ckanext.switzerland.harvester.ftp_storage_adapter import FTPStorageAdapter
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

    def test_get_storage_adapter_when_legacy_config_then_return_ftp_adapted(self):
        self.__build_legacy_config__()
        print(os.getcwd())
        config_resolver = MockConfigResolver(self.ini_file_path, 'app:main')
        factory = StorageAdapterFactory()

        adapter = factory.get_storage_adapter(config_resolver, self.remote_folder, self.config)

        assert isinstance(adapter, FTPStorageAdapter)


