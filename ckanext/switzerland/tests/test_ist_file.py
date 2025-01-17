import os

import pytest
from mock import patch

from ckanext.switzerland.harvester.sbb_harvester import SBBHarvester
from ckanext.switzerland.tests.helpers.mock_ftp_storage_adapter import (
    MockFTPStorageAdapter,
    MockStorageAdapterFactory,
)

from . import data
from .base_ftp_harvester_tests import BaseSBBHarvesterTests


@patch(
    "ckanext.switzerland.harvester.timetable_harvester.StorageAdapterFactory",
    MockStorageAdapterFactory,
)
@patch(
    "ckanext.switzerland.harvester.base_sbb_harvester.StorageAdapterFactory",
    MockStorageAdapterFactory,
)
@patch(
    "ckanext.switzerland.harvester.sbb_harvester.StorageAdapterFactory",
    MockStorageAdapterFactory,
)
class TestIstFileHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBHarvester with ist_file
    """

    harvester_class = SBBHarvester

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
    def test_simple(self):
        filesystem = self.get_filesystem(filename="ist_file.csv")
        path = os.path.join(data.environment, data.folder, "ist_file.csv")
        filesystem.writetext(path, data.ist_file)
        MockFTPStorageAdapter.filesystem = filesystem

        self.run_harvester(ist_file=True, ftp_server="testserver")

        dataset = self.get_dataset()

        self.assertEqual(len(dataset["resources"]), 1)

        self.assertEqual(dataset["resources"][0]["identifier"], "ist_file.csv")
        self.assert_resource_data(dataset["resources"][0]["id"], data.ist_file_output)
