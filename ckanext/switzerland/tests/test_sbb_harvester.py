import json
import os
from datetime import datetime

import pytest
from ckan.lib.munge import munge_name
from ckan.logic import NotFound, get_action
from mock import patch

from ckanext.harvest import model as harvester_model
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
class TestSBBHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBHarvester
    """

    harvester_class = SBBHarvester

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_simple(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        self.assertEqual(len(dataset["resources"]), 1)
        self.assertEqual(dataset["resources"][0]["identifier"], data.filename)

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_existing_dataset(self):
        data.dataset(slug="testslug-other-than-munge-name")

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset1 = self.get_dataset()
        dataset2 = get_action("package_show")(
            {}, {"id": "testslug-other-than-munge-name"}
        )

        self.assertEqual(dataset1["id"], dataset2["id"])
        with self.assertRaises(NotFound):
            get_action("package_show")({}, {"id": munge_name(data.dataset_name)})

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_existing_resource(self):
        """
        Tests harvesting a new file which was not harvested before. Should create a new
        resource and copy some data from the existing one.
        """
        dataset = data.dataset()
        data.resource(dataset=dataset)

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        self.assertEqual(len(dataset["resources"]), 2)
        r1 = dataset["resources"][0]
        r2 = dataset["resources"][1]

        # resources are sorted in descending order
        self.assertEqual(
            r1["title"]["de"], data.filename
        )  # the new resource gets a new name
        self.assertEqual(r2["title"]["de"], "AAAResource")

        # the new resource copies the description from the existing resource
        self.assertEqual(r1["description"]["de"], "AAAResource Desc")
        self.assertEqual(r2["description"]["de"], "AAAResource Desc")

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_existing_resource_same_filename(self):
        """
        Tests harvesting a new file which was not harvested before but manually uploaded
        to ckan.
        Should copy the data from the old resource and delete the old resource.
        """
        dataset = data.dataset()
        data.resource(dataset=dataset, filename=data.filename)

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        self.assertEqual(len(dataset["resources"]), 1)
        resource = dataset["resources"][0]

        self.assertEqual(resource["description"]["de"], "AAAResource Desc")

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_skip_already_harvested_file(self):
        """
        When modified date of file is older than the last harvester run date, the file
        should not be harvested again
        """
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")
        self.run_harvester(ftp_server="testserver")

        self.assertEqual(harvester_model.HarvestSource.count(), 1)
        self.assertEqual(harvester_model.HarvestJob.count(), 2)

        package = self.get_package()

        self.assertEqual(len(package.resources), 1)

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_force_all(self):
        """
        When modified date of file is older than the last harvester run date, the file
        should not be harvested again
        force_all overrides this mechanism and reharvests all files on the ftp server.
        """
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(force_all=True, ftp_server="testserver")

        package = self.get_package()
        resource_id_1 = package.resources[0].id

        self.run_harvester(force_all=True, ftp_server="testserver")
        self.assertEqual(harvester_model.HarvestSource.count(), 1)
        self.assertEqual(harvester_model.HarvestJob.count(), 2)

        package = self.get_package()

        self.assertEqual(len(package.resources), 1)
        resource_id_2 = package.resources[0].id

        self.assertNotEqual(
            resource_id_1,
            resource_id_2,
            "The resource has not been harvested a second time",
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_updated_file_before_last_harvester_run(self):
        """
        When modified date of file is older than the last harvester run date, the file
        should not be harvested again, even if there is no resource with this filename
        on the dataset.
        """
        filesystem = self.get_filesystem()
        MockFTPStorageAdapter.filesystem = filesystem
        self.run_harvester(ftp_server="testserver")

        path = os.path.join(data.environment, data.folder, "NewFile")
        filesystem.writetext(path, data.dataset_content_1)
        filesystem.settimes(path, modified=datetime(2000, 1, 1))
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        self.assertEqual(len(dataset["resources"]), 1)
        self.assertEqual(dataset["resources"][0]["identifier"], "Didok.csv")

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    @pytest.mark.ckan_config("ckan.site_url", "http://odp.test")
    def test_update_version(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        self.run_harvester(ftp_server="testserver")

        package = self.get_package()
        self.assertEqual(len(package.resources), 1)

        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)

        self.run_harvester(ftp_server="testserver")

        package = self.get_package()

        # none of the resources should be deleted
        self.assertEqual(len(package.resources), 2)

        # order should be: newest file first
        self.assertEqual(package.resources[0].extras["identifier"], "20160902.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160901.csv")

        # permalink
        self.assertEqual(
            package.extras["permalink"],
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[0].id
            ),
        )

        self.assert_resource_data(package.resources[0].id, data.dataset_content_2)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_1)

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    @pytest.mark.ckan_config("ckan.site_url", "http://odp.test")
    def test_update_file_of_old_version(self):
        """
        initial state:
        20160901.csv: content 1
        20160902.csv: content 2

        changed state:
        20160901.csv: content 3
        20160902.csv: content 2

        => permalink should still point to the newest file (20160902.csv), and the
        newest file should be on top
        """
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        self.run_harvester(ftp_server="testserver")

        path = os.path.join(data.environment, data.folder, "20160901.csv")
        filesystem.writetext(path, data.dataset_content_3)
        filesystem.settimes(path, modified=datetime.now())

        self.run_harvester(ftp_server="testserver")

        package = self.get_package()

        self.assertEqual(len(package.resources), 2)

        # order should be: newest file first
        self.assertEqual(package.resources[0].extras["identifier"], "20160902.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160901.csv")

        self.assertEqual(
            package.extras["permalink"],
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[0].id
            ),
        )

        self.assert_resource_data(package.resources[0].id, data.dataset_content_2)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_3)

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    @pytest.mark.ckan_config("ckan.site_url", "http://odp.test")
    def test_update_file_of_newest_version(self):
        """
        initial state:
        20160901.csv: content 1
        20160902.csv: content 2

        changed state:
        20160901.csv: content 1
        20160902.csv: content 3
        => updated file should be on top including permalink pointing to it
        """
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        self.run_harvester(ftp_server="testserver")

        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_3)
        filesystem.settimes(path, modified=datetime.now())

        self.run_harvester(ftp_server="testserver")

        package = self.get_package()

        self.assertEqual(len(package.resources), 2)

        # order should be: newest file first
        self.assertEqual(package.resources[0].extras["identifier"], "20160902.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160901.csv")

        self.assertEqual(
            package.extras["permalink"],
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[0].id
            ),
        )

        self.assert_resource_data(package.resources[0].id, data.dataset_content_3)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_1)

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    @pytest.mark.ckan_config("ckan.site_url", "http://odp.test")
    def test_order_permalink_regex(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        path = os.path.join(data.environment, data.folder, "1111Resource.csv")
        filesystem.writetext(path, data.dataset_content_3)
        path = os.path.join(data.environment, data.folder, "9999Resource.csv")
        filesystem.writetext(path, data.dataset_content_3)
        self.run_harvester(resource_regex=r"\d{8}.csv", ftp_server="testserver")

        package = self.get_package()

        self.assertEqual(len(package.resources), 4)

        self.assertEqual(package.resources[0].extras["identifier"], "1111Resource.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "9999Resource.csv")
        self.assertEqual(package.resources[2].extras["identifier"], "20160902.csv")
        self.assertEqual(package.resources[3].extras["identifier"], "20160901.csv")

        self.assertEqual(
            package.extras["permalink"],
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[2].id
            ),
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    # cleanup tests
    def test_max_resources(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        path = os.path.join(data.environment, data.folder, "20160903.csv")
        filesystem.writetext(path, data.dataset_content_3)
        self.run_harvester(max_resources=3, ftp_server="testserver")

        path = os.path.join(data.environment, data.folder, "20160904.csv")
        filesystem.writetext(path, data.dataset_content_3)

        self.run_harvester(max_resources=3, ftp_server="testserver")

        package = self.get_package()

        self.assertEqual(len(package.resources), 3)

        self.assertEqual(package.resources[0].extras["identifier"], "20160904.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160903.csv")
        self.assertEqual(package.resources[2].extras["identifier"], "20160902.csv")

    @pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index", "harvest_setup")
    def test_filter_regex(self):
        filesystem = self.get_filesystem(filename="File.zip")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "Invalid.csv")
        filesystem.writetext(path, data.dataset_content_2)

        self.run_harvester(filter_regex=r".*\.zip", ftp_server="testserver")

        package = self.get_package()
        self.assertEqual(len(package.resources), 1)

        self.assert_resource_data(package.resources[0].id, data.dataset_content_1)
        self.assertEqual(package.resources[0].extras["identifier"], "File.zip")

    def test_validate_regex_fail(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        harvester = SBBHarvester()
        with self.assertRaises(Exception):
            harvester.validate_config(
                json.dumps(
                    {
                        "dataset": data.dataset_name,
                        "environment": data.environment,
                        "folder": data.folder,
                        "filter_regex": "*",
                    }
                )
            )

    def test_validate_regex_ok(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        harvester = SBBHarvester()
        harvester.validate_config(
            json.dumps(
                {
                    "dataset": data.dataset_name,
                    "environment": data.environment,
                    "folder": data.folder,
                    "filter_regex": ".*",
                }
            )
        )
