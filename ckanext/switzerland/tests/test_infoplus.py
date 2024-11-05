# coding: utf-8

import json
import os
from io import BytesIO
from zipfile import ZipFile

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
class TestInfoplusHarvester(BaseSBBHarvesterTests):
    """
    Integration test for TimetableHarvester with Infoplus files
    """

    harvester_class = TimetableHarvester

    def test_config(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        harvester = TimetableHarvester()
        harvester.validate_config(
            json.dumps(
                {
                    "dataset": data.dataset_name,
                    "environment": data.environment,
                    "folder": data.folder,
                    "timetable_regex": r"FP(\d{4}).*\.zip",
                    "infoplus": {
                        "year": 2015,
                        "dataset": "Station List",
                        "files": {"BAHNHOF": data.infoplus_config},
                    },
                    "storage_adapter": "FTP",
                    "ftp_server": "testserver",
                }
            )
        )

    @pytest.mark.ckan_config(
        "ckan.plugins", "ogdch ogdch_pkg harvest timetable_harvester fluent scheming_datasets activity"
    )
    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
    def test_simple(self):
        filesystem = self.get_filesystem(filename="FP2016_Jahresfahrplan.zip")
        MockFTPStorageAdapter.filesystem = filesystem

        path = os.path.join(data.environment, data.folder, "FP2015_Jahresfahrplan.zip")
        filesystem.writetext(path, data.dataset_content_1)

        path = os.path.join(
            data.environment, data.folder, "FP2015_Fahrplan_20150901.zip"
        )
        filesystem.writetext(path, data.dataset_content_1)

        path = os.path.join(
            data.environment, data.folder, "FP2015_Fahrplan_20151001.zip"
        )
        f = BytesIO()
        zipfile = ZipFile(f, "w")
        zipfile.writestr("BAHNHOF", data.bahnhof_file)
        zipfile.close()
        filesystem.writebytes(path, f.getvalue())

        self.run_harvester(
            dataset="Timetable {year}",
            timetable_regex=r"FP(\d{4}).*\.zip",
            resource_regex=r"FP(\d{4})_Fahrplan_\d{8}\.zip",
            infoplus={
                "year": 2015,
                "dataset": "Station List",
                "files": {"BAHNHOF": data.infoplus_config},
            },
            ftp_server="testserver",
        )

        dataset = self.get_dataset(name="Station List")

        # There is no dataset to copy metadata data from, so we use the dataset's
        # identifier as the title and use the default value for relations.
        self.assert_dataset_data(
            dataset,
            identifier="Station List",
            title={
                "de": "Station List",
                "it": "Station List",
                "fr": "Station List",
                "en": "Station List",
            },
            relations=[],
        )

        self.assertEqual(len(dataset["resources"]), 1)

        self.assertIn("issued", dataset)
        self.assertIn("modified", dataset)
        self.assertIsNotNone(dataset["issued"])
        self.assertIsNotNone(dataset["modified"])

        self.assertEqual(dataset["resources"][0]["identifier"], "BAHNHOF.csv")
        self.assert_resource_data(dataset["resources"][0]["id"], data.bahnhof_file_csv)
