import os

from mock import patch

from ckanext.switzerland.harvester.timetable_harvester import TimetableHarvester
from ckanext.switzerland.tests import data
from ckanext.switzerland.tests.helpers.mock_ftp_storage_adapter import (
    MockFTPStorageAdapter,
)

from .base_ftp_harvester_tests import BaseSBBHarvesterTests


@patch(
    "ckanext.switzerland.harvester.timetable_harvester.FTPStorageAdapter",
    MockFTPStorageAdapter,
)
@patch(
    "ckanext.switzerland.harvester.base_sbb_harvester.FTPStorageAdapter",
    MockFTPStorageAdapter,
)
class TestTimetableHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBHarvester
    """

    harvester_class = TimetableHarvester

    def test_simple(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem(
            filename="FP2016_Jahresfahrplan.zip"
        )
        self.run_harvester(
            dataset="Timetable {year}", timetable_regex=r"FP(\d\d\d\d).*"
        )

        dataset = self.get_dataset(name="Timetable 2016")

        self.assertEqual(len(dataset["resources"]), 1)

    def test_multi_year(self):
        filesystem = self.get_filesystem(filename="FP2016_Jahresfahrplan.zip")
        MockFTPStorageAdapter.filesystem = filesystem

        path = os.path.join(data.environment, data.folder, "FP2015_Jahresfahrplan.zip")
        filesystem.setcontents(path, data.dataset_content_3)

        path = os.path.join(data.environment, data.folder, "InvalidFile")
        filesystem.setcontents(path, data.dataset_content_2)

        self.run_harvester(
            dataset="Timetable {year}", timetable_regex=r"FP(\d\d\d\d).*"
        )

        dataset1 = self.get_dataset(name="Timetable 2016")
        self.assertEqual(len(dataset1["resources"]), 1)
        self.assert_resource_data(
            dataset1["resources"][0]["id"], data.dataset_content_1
        )

        dataset2 = self.get_dataset(name="Timetable 2015")
        self.assertEqual(len(dataset2["resources"]), 1)
        self.assert_resource_data(
            dataset2["resources"][0]["id"], data.dataset_content_3
        )
