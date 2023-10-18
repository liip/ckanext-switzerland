import json
from datetime import datetime

import os
from ckan.lib.munge import munge_name
from ckan.logic import get_action, NotFound
from ckanext.harvest import model as harvester_model
from ckanext.switzerland.harvester.sbb_harvester import SBBHarvester
from ckanext.switzerland.tests.helpers.mock_ftp_storage_adapter import (
    MockFTPStorageAdapter,
)
from mock import patch
from nose.tools import assert_equal, assert_raises

from . import data
from .base_ftp_harvester_tests import BaseSBBHarvesterTests


@patch(
    "ckanext.switzerland.harvester.sbb_harvester.FTPStorageAdapter",
    MockFTPStorageAdapter,
)
@patch(
    "ckanext.switzerland.harvester.base_sbb_harvester.FTPStorageAdapter",
    MockFTPStorageAdapter,
)
class TestSBBHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBHarvester
    """

    harvester_class = SBBHarvester

    def test_simple(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset = self.get_dataset()

        assert_equal(len(dataset["resources"]), 1)
        assert_equal(dataset["resources"][0]["identifier"], data.filename)

    def test_existing_dataset(self):
        data.dataset(slug="testslug-other-than-munge-name")

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset1 = self.get_dataset()
        dataset2 = get_action("package_show")(
            {}, {"id": "testslug-other-than-munge-name"}
        )

        assert_equal(dataset1["id"], dataset2["id"])
        with assert_raises(NotFound):
            get_action("package_show")({}, {"id": munge_name(data.dataset_name)})

    def test_existing_resource(self):
        """
        Tests harvesting a new file which was not harvested before. Should create a new resource
        and copy some data from the existing one.
        """
        dataset = data.dataset()
        data.resource(dataset=dataset)

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset = self.get_dataset()

        assert_equal(len(dataset["resources"]), 2)
        r1 = dataset["resources"][0]
        r2 = dataset["resources"][1]

        # resources are sorted in descending order
        assert_equal(
            r1["title"]["de"], data.filename
        )  # the new resource gets a new name
        assert_equal(r2["title"]["de"], "AAAResource")

        # the new resource copies the description from the existing resource
        assert_equal(r1["description"]["de"], "AAAResource Desc")
        assert_equal(r2["description"]["de"], "AAAResource Desc")

    def test_existing_resource_same_filename(self):
        """
        Tests harvesting a new file which was not harvested before but manually uploaded to ckan.
        Should copy the data from the old resource and delete the old resource.
        """
        dataset = data.dataset()
        data.resource(dataset=dataset, filename=data.filename)

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset = self.get_dataset()

        assert_equal(len(dataset["resources"]), 1)
        resource = dataset["resources"][0]

        assert_equal(resource["title"]["de"], "AAAResource")
        assert_equal(resource["description"]["de"], "AAAResource Desc")

    def test_skip_already_harvested_file(self):
        """
        When modified date of file is older than the last harvester run date, the file should not be harvested again
        """
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester()
        self.run_harvester()

        assert_equal(harvester_model.HarvestSource.count(), 1)
        assert_equal(harvester_model.HarvestJob.count(), 2)

        package = self.get_package()

        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 1)

    def test_force_all(self):
        """
        When modified date of file is older than the last harvester run date, the file should not be harvested again
        force_all overrides this mechanism and reharvests all files on the ftp server.
        """
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(force_all=True)
        self.run_harvester(force_all=True)

        assert_equal(harvester_model.HarvestSource.count(), 1)
        assert_equal(harvester_model.HarvestJob.count(), 2)

        package = self.get_package()

        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 2)

    def test_updated_file_before_last_harvester_run(self):
        """
        When modified date of file is older than the last harvester run date, the file should not be harvested again,
        except when the file is missing in the dataset, that is what we are testing here.
        """
        filesystem = self.get_filesystem()
        MockFTPStorageAdapter.filesystem = filesystem
        self.run_harvester()

        path = os.path.join(data.environment, data.folder, "NewFile")
        filesystem.setcontents(path, data.dataset_content_1)
        filesystem.settimes(path, modified_time=datetime(2000, 1, 1))
        self.run_harvester()

        dataset = self.get_dataset()

        assert_equal(len(dataset["resources"]), 2)

    def test_update_version(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        self.run_harvester()

        package = self.get_package()
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 1)

        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_2)

        self.run_harvester()

        package = self.get_package()

        # none of the resources should be deleted
        assert_equal(len(package.resources), 2)
        assert_equal(len(package.resources_all), 2)

        # order should be: newest file first
        assert_equal(package.resources[0].extras["identifier"], "20160902.csv")
        assert_equal(package.resources[1].extras["identifier"], "20160901.csv")

        # permalink
        assert_equal(
            package.permalink,
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[0].id
            ),
        )

        self.assert_resource_data(package.resources[0].id, data.dataset_content_2)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_1)

    def test_update_file_of_old_version(self):
        """
        initial state:
        20160901.csv: content 1
        20160902.csv: content 2

        changed state:
        20160901.csv: content 3
        20160902.csv: content 2

        => permalink should still point to the newest file (20160902.csv), and the newest file should be on top
        """
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_2)
        self.run_harvester()

        path = os.path.join(data.environment, data.folder, "20160901.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        filesystem.settimes(path, modified_time=datetime.now())

        self.run_harvester()

        package = self.get_package()

        # there should be 3 resources now, 1 of them deleted
        assert_equal(len(package.resources), 2)
        assert_equal(len(package.resources_all), 3)

        # order should be: newest file first
        assert_equal(package.resources[0].extras["identifier"], "20160902.csv")
        assert_equal(package.resources[1].extras["identifier"], "20160901.csv")

        assert_equal(
            package.permalink,
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[0].id
            ),
        )

        self.assert_resource_data(package.resources[0].id, data.dataset_content_2)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_3)

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
        filesystem.setcontents(path, data.dataset_content_2)
        self.run_harvester()

        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        filesystem.settimes(path, modified_time=datetime.now())

        self.run_harvester()

        package = self.get_package()

        # there should be 3 resources now, 1 of them deleted
        assert_equal(len(package.resources), 2)
        assert_equal(len(package.resources_all), 3)

        # order should be: newest file first
        assert_equal(package.resources[0].extras["identifier"], "20160902.csv")
        assert_equal(package.resources[1].extras["identifier"], "20160901.csv")

        assert_equal(
            package.permalink,
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[0].id
            ),
        )

        self.assert_resource_data(package.resources[0].id, data.dataset_content_3)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_1)

    def test_order_permalink_regex(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_2)
        path = os.path.join(data.environment, data.folder, "1111Resource.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        path = os.path.join(data.environment, data.folder, "9999Resource.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        self.run_harvester(resource_regex="\d{8}.csv")

        package = self.get_package()

        assert_equal(len(package.resources), 4)

        assert_equal(package.resources[0].extras["identifier"], "9999Resource.csv")
        assert_equal(package.resources[1].extras["identifier"], "1111Resource.csv")
        assert_equal(package.resources[2].extras["identifier"], "20160902.csv")
        assert_equal(package.resources[3].extras["identifier"], "20160901.csv")

        assert_equal(
            package.permalink,
            "http://odp.test/dataset/{}/resource/{}/download/20160902.csv".format(
                package.id, package.resources[2].id
            ),
        )

    # cleanup tests
    def test_max_resources(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_2)
        path = os.path.join(data.environment, data.folder, "20160903.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        self.run_harvester(max_resources=3)

        path = os.path.join(data.environment, data.folder, "20160904.csv")
        filesystem.setcontents(path, data.dataset_content_3)

        self.run_harvester(max_resources=3)

        package = self.get_package()

        assert_equal(len(package.resources), 3)
        assert_equal(len(package.resources_all), 4)

        assert_equal(package.resources[0].extras["identifier"], "20160904.csv")
        assert_equal(package.resources[1].extras["identifier"], "20160903.csv")
        assert_equal(package.resources[2].extras["identifier"], "20160902.csv")

        for resource in package.resources_all:
            if resource.extras["identifier"] == "20160901.csv":
                self.assert_resource_deleted(resource)
            else:
                self.assert_resource_exists(resource)

    def test_max_resources_revisions(self):
        """
        there are multiple revisions of file 20160901.csv, all of them should be deleted
        """
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        self.run_harvester(max_resources=3)

        path = os.path.join(data.environment, data.folder, "20160901.csv")
        filesystem.setcontents(path, data.dataset_content_2)
        filesystem.settimes(path, modified_time=datetime.now())
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        path = os.path.join(data.environment, data.folder, "20160903.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        self.run_harvester(max_resources=3)

        package = self.get_package()
        assert_equal(len(package.resources), 3)
        assert_equal(len(package.resources_all), 4)

        path = os.path.join(data.environment, data.folder, "20160904.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        self.run_harvester(max_resources=3)

        package = self.get_package()
        assert_equal(len(package.resources), 3)
        assert_equal(len(package.resources_all), 5)

        for resource in package.resources_all:
            if resource.extras["identifier"] == "20160901.csv":
                self.assert_resource_deleted(resource)
            else:
                self.assert_resource_exists(resource)

    def test_max_resources_redownload_files(self):
        """
        If resources get deleted by max_resources, we should not redownload them from ftp.
        """
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        path = os.path.join(data.environment, data.folder, "20160903.csv")
        filesystem.setcontents(path, data.dataset_content_3)
        path = os.path.join(data.environment, data.folder, "20160904.csv")
        filesystem.setcontents(path, data.dataset_content_4)

        self.run_harvester(max_resources=3)

        package = self.get_package()
        assert_equal(len(package.resources), 3)
        assert_equal(len(package.resources_all), 4)

        self.run_harvester(max_resources=3)

        package = self.get_package()
        assert_equal(len(package.resources), 3)
        assert_equal(len(package.resources_all), 4)

    def test_max_revisions(self):
        filesystem = self.get_filesystem()
        MockFTPStorageAdapter.filesystem = filesystem

        path = os.path.join(data.environment, data.folder, data.filename)
        filesystem.settimes(path, modified_time=datetime.now())
        filesystem.setcontents(path, data.dataset_content_1)
        self.run_harvester(max_revisions=3)
        package = self.get_package()
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 1)

        filesystem.settimes(path, modified_time=datetime.now())
        filesystem.setcontents(path, data.dataset_content_2)
        self.run_harvester(max_revisions=3)
        package = self.get_package()
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 2)

        filesystem.settimes(path, modified_time=datetime.now())
        filesystem.setcontents(path, data.dataset_content_3)
        self.run_harvester(max_revisions=3)
        package = self.get_package()
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 3)

        filesystem.settimes(path, modified_time=datetime.now())
        filesystem.setcontents(path, data.dataset_content_4)
        self.run_harvester(max_revisions=3)
        package = self.get_package()
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 4)

        resources = sorted(package.resources_all, key=lambda r: r.created)

        self.assert_resource_deleted(resources[0])
        self.assert_resource_exists(resources[1])
        self.assert_resource_exists(resources[2])
        self.assert_resource_exists(resources[3])

        self.assert_resource_data(resources[1].id, data.dataset_content_2)
        self.assert_resource_data(resources[2].id, data.dataset_content_3)
        self.assert_resource_data(resources[3].id, data.dataset_content_4)

        self.assert_resource_exists(package.resources[0])
        self.assert_resource_data(package.resources[0].id, data.dataset_content_4)

    def test_filter_regex(self):
        filesystem = self.get_filesystem(filename="File.zip")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "Invalid.csv")
        filesystem.setcontents(path, data.dataset_content_2)

        self.run_harvester(filter_regex=".*\.zip")

        package = self.get_package()
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 1)

        self.assert_resource_data(package.resources[0].id, data.dataset_content_1)
        assert_equal(package.resources[0].extras["identifier"], "File.zip")

    def test_validate_regex_fail(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        harvester = SBBHarvester()
        with assert_raises(Exception):
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
