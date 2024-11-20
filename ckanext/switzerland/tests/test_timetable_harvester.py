import os

import pytest
from mock import patch

from ckanext.switzerland.harvester.timetable_harvester import TimetableHarvester
from ckanext.switzerland.tests import data
from ckanext.switzerland.tests.helpers.mock_ftp_storage_adapter import (
    MockFTPStorageAdapter,
    MockStorageAdapterFactory,
)

from .base_ftp_harvester_tests import BaseSBBHarvesterTests


@patch(
    "ckanext.switzerland.harvester.timetable_harvester.StorageAdapterFactory",
    MockStorageAdapterFactory,
)
@patch(
    "ckanext.switzerland.harvester.base_sbb_harvester.StorageAdapterFactory",
    MockStorageAdapterFactory,
)
class TestTimetableHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBHarvester
    """

    harvester_class = TimetableHarvester

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
    def test_simple(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem(
            filename="FP2016_Jahresfahrplan.zip"
        )
        self.run_harvester(
            dataset="Timetable {year}",
            timetable_regex=r"FP(\d\d\d\d).*",
            ftp_server="testserver",
        )

        dataset = self.get_dataset(name="Timetable 2016")
        self.assert_dataset_data(
            dataset,
            identifier="Timetable 2016",
            title={
                "de": "Timetable 2016",
                "it": "Timetable 2016",
                "fr": "Timetable 2016",
                "en": "Timetable 2016",
            },
            relations=[],
        )

        self.assertEqual(len(dataset["resources"]), 1)

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
    def test_multi_year(self):
        filesystem = self.get_filesystem(filename="FP2016_Jahresfahrplan.zip")
        MockFTPStorageAdapter.filesystem = filesystem

        path = os.path.join(data.environment, data.folder, "FP2015_Jahresfahrplan.zip")
        filesystem.writetext(path, data.dataset_content_3)

        path = os.path.join(data.environment, data.folder, "InvalidFile")
        filesystem.writetext(path, data.dataset_content_2)

        self.run_harvester(
            dataset="Timetable {year}",
            timetable_regex=r"FP(\d\d\d\d).*",
            ftp_server="testserver",
        )

        dataset1 = self.get_dataset(name="Timetable 2016")
        self.assert_dataset_data(
            dataset1,
            identifier="Timetable 2016",
            title={
                "de": "Timetable 2016",
                "it": "Timetable 2016",
                "fr": "Timetable 2016",
                "en": "Timetable 2016",
            },
            relations=[],
        )
        self.assertEqual(len(dataset1["resources"]), 1)
        self.assert_resource_data(
            dataset1["resources"][0]["id"], data.dataset_content_1
        )

        dataset2 = self.get_dataset(name="Timetable 2015")
        self.assert_dataset_data(
            dataset2,
            identifier="Timetable 2015",
            title={
                "de": "Timetable 2015",
                "it": "Timetable 2015",
                "fr": "Timetable 2015",
                "en": "Timetable 2015",
            },
            relations=[],
        )
        self.assertEqual(len(dataset2["resources"]), 1)
        self.assert_resource_data(
            dataset2["resources"][0]["id"], data.dataset_content_3
        )
