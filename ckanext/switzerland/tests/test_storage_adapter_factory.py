import unittest
import os
from ckanext.switzerland.harvester.ftp_storage_adapter import FTPStorageAdapter
from ckanext.switzerland.harvester.s3_storage_adapter import S3StorageAdapter
from .helpers.mock_config_resolver import MockConfigResolver

# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.harvester.storage_adapter_factory import StorageAdapterFactory

# -----------------------------------------------------------------------

CONFIG_SECTION = "app:main"


class TestStorageAdapterFactory(unittest.TestCase):
    config = {}
    remote_folder = ""
    ini_file_path = "./ckanext/switzerland/tests/config/nosetest.ini"

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def __build_base_config__(self):
        self.config = {
            "environment": "Test",
            "folder": "DiDok",
            "dataset": "DiDok",
            "max_resources": 30,
            "max_revisions": 30,
            "filter_regex": ".*\\.xls",
            "resource_regex": "\\d{8}-Ist-File\\.xls",
            "ist_file": True,
        }

    def __build_legacy_config__(self):
        self.config["ftp_server"] = "mainserver"

    def __build_s3_config__(self):
        self.config["storage_adapter"] = "S3"
        self.config["bucket"] = "main_bucket"

    def __build_ftp_config__(self):
        self.config["storage_adapter"] = "FTP"
        self.config["ftp_server"] = "mainserver"

    def __build_unsupported_config__(self):
        self.config["storage_adapter"] = "unsupported"

    def __build_adapter_type_none_config__(self):
        self.config["storage_adapter"] = None

    def test_get_storage_adapter_when_legacy_config_then_returns_ftp_adapter(self):
        self.__build_legacy_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        factory = StorageAdapterFactory(config_resolver)

        adapter = factory.get_storage_adapter(self.remote_folder, self.config)

        assert isinstance(adapter, FTPStorageAdapter)

    def test_get_storage_adapter_when_s3_config_then_returns_s3_adapter(self):
        self.__build_s3_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        factory = StorageAdapterFactory(config_resolver)

        adapter = factory.get_storage_adapter(self.remote_folder, self.config)

        assert isinstance(adapter, S3StorageAdapter)

    def test_get_storage_adapter_when_adapter_lower_case_then_returns_adapter(self):
        self.__build_s3_config__()
        self.config["storage_adapter"] = self.config["storage_adapter"].lower()
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        factory = StorageAdapterFactory(config_resolver)

        adapter = factory.get_storage_adapter(self.remote_folder, self.config)

        assert isinstance(adapter, S3StorageAdapter)

    def test_get_storage_adapter_when_ftp_config_then_returns_ftp_adapter(self):
        self.__build_ftp_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        factory = StorageAdapterFactory(config_resolver)

        adapter = factory.get_storage_adapter(self.remote_folder, self.config)

        assert isinstance(adapter, FTPStorageAdapter)

    def test_get_storage_adapter_when_unsupported_config_then_throws_exception(self):
        self.__build_unsupported_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        factory = StorageAdapterFactory(config_resolver)

        self.assertRaises(
            Exception, factory.get_storage_adapter, self.remote_folder, self.config
        )

    def test_get_storage_adapter_when_storage_adapter_type_none_then_throws_exception(
        self,
    ):
        self.__build_adapter_type_none_config__()
        config_resolver = MockConfigResolver(self.ini_file_path, CONFIG_SECTION)
        factory = StorageAdapterFactory(config_resolver)

        self.assertRaises(
            Exception, factory.get_storage_adapter, self.remote_folder, self.config
        )
