import json
import os
from datetime import datetime
from time import sleep
from zoneinfo import ZoneInfo

import pytest
import time_machine
from ckan.lib.munge import munge_filename, munge_name
from ckan.logic import NotFound, get_action
from ckan.tests import factories
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
@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
class TestSBBHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBHarvester
    """

    harvester_class = SBBHarvester

    def test_simple(self):
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        self.assertEqual(len(dataset["resources"]), 1)
        self.assertEqual(dataset["resources"][0]["identifier"], data.filename)

        # There is no dataset to copy metadata data from, so we use the dataset's
        # identifier as the title and use the default value for relations.
        self.assert_dataset_data(
            dataset,
            title={"de": "Dataset", "it": "Dataset", "fr": "Dataset", "en": "Dataset"},
            relations=[],
            publisher={
                "url": "",
                "name": {
                    "de": "",
                    "fr": "",
                    "it": "",
                    "en": "",
                },
            },
        )

    def test_existing_dataset(self):
        data.dataset(slug="testslug-other-than-munge-name")

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset1 = self.get_dataset()
        dataset2 = get_action("package_show")(
            {}, {"id": "testslug-other-than-munge-name"}
        )

        self.assertEqual(dataset1["id"], dataset2["id"])
        self.assert_dataset_data(dataset2)

        with self.assertRaises(NotFound):
            get_action("package_show")({}, {"id": munge_name(data.dataset_name)})

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

        self.assert_dataset_data(dataset)

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

        # the new resource copies the license from the existing resource
        self.assertEqual(
            r1["license"],
            "http://dcat-ap.ch/vocabulary/licenses/terms_open",
        )
        self.assertEqual(
            r2["license"],
            "http://dcat-ap.ch/vocabulary/licenses/terms_open",
        )

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

        self.assert_dataset_data(dataset)

        self.assertEqual(len(dataset["resources"]), 1)
        resource = dataset["resources"][0]

        self.assertEqual(resource["description"]["de"], "AAAResource Desc")
        self.assertEqual(
            resource["license"],
            "http://dcat-ap.ch/vocabulary/licenses/terms_open",
        )

    def test_replace_same_filename_preserves_format_from_old_resource(self):
        """When a file replaces an existing resource, ``format`` is copied from the old resource."""
        dataset = data.dataset()
        res = data.resource(dataset=dataset, filename=data.filename)
        get_action("resource_patch")(
            {"user": factories.Sysadmin()["name"]},
            {"id": res["id"], "format": "GEOJSON"},
        )

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()
        self.assertEqual(len(dataset["resources"]), 1)
        resource = dataset["resources"][0]
        self.assertEqual(resource["identifier"], data.filename)
        self.assert_resource_format_ignore_case(resource["format"], "geojson")

    def test_new_resource_keeps_mime_format_with_fallback_metadata(self):
        """With no same-filename resource, ``format`` comes from MIME; other metadata may still be copied from a template resource."""
        dataset = data.dataset()
        res = data.resource(dataset=dataset)
        get_action("resource_patch")(
            {"user": factories.Sysadmin()["name"]},
            {"id": res["id"], "format": "GEOJSON"},
        )

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()
        self.assertEqual(len(dataset["resources"]), 2)
        new_res = next(
            r for r in dataset["resources"] if r["identifier"] == data.filename
        )
        self.assert_resource_format_ignore_case(new_res["format"], "csv")
        self.assertEqual(
            new_res["rights"],
            res["rights"],
            "Non-format fields should still be inherited from the fallback resource.",
        )

    def test_copied_metadata_only_affects_new_harvested_resource(self):
        """Only the resource created from the FTP file gets metadata copied from the fallback; other resources are unchanged."""
        dataset = data.dataset()
        aaa = data.resource(dataset=dataset)
        get_action("resource_patch")(
            {"user": factories.Sysadmin()["name"]},
            {"id": aaa["id"], "format": "GEOJSON"},
        )

        factories.Resource(
            package_id=dataset["id"],
            identifier="Other.csv",
            format="XML",
            title={"de": "Other", "en": "Other", "fr": "Other", "it": "Other"},
            name={"de": "Other", "en": "Other", "fr": "Other", "it": "Other"},
            description={
                "de": "Other desc",
                "en": "Other desc",
                "fr": "Other desc",
                "it": "Other desc",
            },
            state="active",
            rights="Other (Open)",
            license="http://dcat-ap.ch/vocabulary/licenses/terms_open",
            coverage="Coverage",
            url="http://odp.test/dataset/testdataset/resource/download/{}".format(
                munge_filename("Other.csv")
            ),
        )

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver", resource_sort_order="asc")

        dataset = self.get_dataset()
        self.assertEqual(len(dataset["resources"]), 3)
        by_identifier = {r["identifier"]: r for r in dataset["resources"]}

        self.assert_resource_format_ignore_case(
            by_identifier[data.filename]["format"],
            "csv",
            "Harvested file should use MIME format, not the template resource format",
        )
        self.assertEqual(
            by_identifier[data.filename]["rights"],
            by_identifier["AAAResource"]["rights"],
            "Non-format metadata should still be copied from the fallback template resource.",
        )
        self.assertEqual(
            by_identifier["Other.csv"]["format"],
            "XML",
            "Another resource on the dataset must not be updated by the harvest import",
        )
        self.assert_resource_format_ignore_case(
            by_identifier["AAAResource"]["format"],
            "geojson",
        )

    def test_replace_same_filename_leaves_other_resources_untouched(self):
        """Replacing a harvested file must not change ``format`` on other resources."""
        dataset = data.dataset()
        didok_res = data.resource(dataset=dataset, filename=data.filename)
        get_action("resource_patch")(
            {"user": factories.Sysadmin()["name"]},
            {"id": didok_res["id"], "format": "GEOJSON"},
        )

        factories.Resource(
            package_id=dataset["id"],
            identifier="Other.csv",
            format="XML",
            title={"de": "Other", "en": "Other", "fr": "Other", "it": "Other"},
            name={"de": "Other", "en": "Other", "fr": "Other", "it": "Other"},
            description={
                "de": "Other desc",
                "en": "Other desc",
                "fr": "Other desc",
                "it": "Other desc",
            },
            state="active",
            rights="Other (Open)",
            license="http://dcat-ap.ch/vocabulary/licenses/terms_open",
            coverage="Coverage",
            url="http://odp.test/dataset/testdataset/resource/download/{}".format(
                munge_filename("Other.csv")
            ),
        )

        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()
        self.assertEqual(len(dataset["resources"]), 2)
        by_identifier = {r["identifier"]: r for r in dataset["resources"]}

        self.assert_resource_format_ignore_case(
            by_identifier[data.filename]["format"],
            "geojson",
        )
        self.assertEqual(
            by_identifier["Other.csv"]["format"],
            "XML",
            "Sibling resource must not be modified when another resource is replaced",
        )

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

        # Wait a short time to be sure that there's a big enough difference between the
        # last harvester run and the file's modified date
        sleep(3)
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

        # Wait a short time to be sure that there's a big enough difference between the
        # last harvester run and the file's modified date
        sleep(3)
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

    def test_resource_sort_order_default_is_desc(self):
        """If resource_sort_order is omitted from config, resources are ordered desc."""
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        self.run_harvester(ftp_server="testserver")

        package = self.get_package()
        self.assertEqual(package.resources[0].extras["identifier"], "20160902.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160901.csv")

    def test_resource_sort_order_desc_explicit(self):
        """resource_sort_order 'desc' sorts identifiers descending (newest name first)."""
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        self.run_harvester(ftp_server="testserver", resource_sort_order="desc")

        package = self.get_package()
        self.assertEqual(package.resources[0].extras["identifier"], "20160902.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160901.csv")

    def test_resource_sort_order_asc(self):
        """resource_sort_order 'asc' sorts identifiers ascending."""
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        self.run_harvester(ftp_server="testserver", resource_sort_order="asc")

        package = self.get_package()
        self.assertEqual(package.resources[0].extras["identifier"], "20160901.csv")
        self.assertEqual(package.resources[1].extras["identifier"], "20160902.csv")
        self.assertEqual(
            package.extras["permalink"],
            "http://odp.test/dataset/{}/resource/{}/download/20160901.csv".format(
                package.id, package.resources[0].id
            ),
        )

    def test_resource_sort_order_ignored_when_resource_regex_in_config(self):
        """If resource_regex is present in the harvester config, resource_sort_order is
        ignored: matched files keep descending identifier order even when asc is set.
        Unmatched files still precede the matched block (unchanged).
        """
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160902.csv")
        filesystem.writetext(path, data.dataset_content_2)
        path = os.path.join(data.environment, data.folder, "1111Resource.csv")
        filesystem.writetext(path, data.dataset_content_3)
        path = os.path.join(data.environment, data.folder, "9999Resource.csv")
        filesystem.writetext(path, data.dataset_content_3)

        self.run_harvester(
            resource_regex=r"\d{8}.csv",
            resource_sort_order="asc",
            ftp_server="testserver",
        )
        package = self.get_package()
        self.assertEqual(
            [r.extras["identifier"] for r in package.resources],
            [
                "1111Resource.csv",
                "9999Resource.csv",
                "20160902.csv",
                "20160901.csv",
            ],
        )

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

    def test_resource_license_no_existing_resource(self):
        """With no existing resource to copy metadata from, the default license should
        be used."""
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        resource = dataset["resources"][0]
        self.assertEqual(
            resource["license"],
            "http://dcat-ap.ch/vocabulary/licenses/terms_by",
        )

    def test_resource_license_no_existing_resource_astra_organization(self):
        """With no existing resource to copy metadata from, the default license for the
        ASTRA organization should be used."""
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver", org_name="astra")

        dataset = self.get_dataset()

        resource = dataset["resources"][0]
        self.assertEqual(
            resource["license"],
            "http://dcat-ap.ch/vocabulary/licenses/terms_by_ask",
        )

    def test_resource_formats(self):
        filesystem = self.get_filesystem(filename="20160901.csv")
        MockFTPStorageAdapter.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, "20160901.xml")
        filesystem.writetext(path, data.dataset_content_2)
        path = os.path.join(data.environment, data.folder, "20160901.json")
        filesystem.writetext(path, data.dataset_content_3)
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()
        self.assertEqual(len(dataset["resources"]), 3)

        result = get_action("package_search")({}, {"facet.field": ["res_format"]})
        res_format_facet = result["facets"]["res_format"]
        self.assertDictEqual(res_format_facet, {"CSV": 1, "JSON": 1, "XML": 1})

        csv_resource = next(
            (
                resource
                for resource in dataset["resources"]
                if resource["identifier"] == "20160901.csv"
            ),
            None,
        )
        self.assertEqual(csv_resource["identifier"], "20160901.csv")
        self.assertEqual(csv_resource["format"], "CSV")
        self.assertEqual(csv_resource["media_type"], "text/csv")
        self.assertEqual(csv_resource["mimetype"], "text/csv")
        # mimetype_inner is None, so CKAN doesn't save it on the resource
        self.assertNotIn("mimetype_inner", csv_resource)

        xml_resource = next(
            (
                resource
                for resource in dataset["resources"]
                if resource["identifier"] == "20160901.xml"
            ),
            None,
        )
        self.assertEqual(xml_resource["identifier"], "20160901.xml")
        self.assertEqual(xml_resource["format"], "XML")
        self.assertEqual(xml_resource["media_type"], "application/xml")
        self.assertEqual(xml_resource["mimetype"], "application/xml")
        # mimetype_inner is None, so CKAN doesn't save it on the resource
        self.assertNotIn("mimetype_inner", xml_resource)

        json_resource = next(
            (
                resource
                for resource in dataset["resources"]
                if resource["identifier"] == "20160901.json"
            ),
            None,
        )
        self.assertEqual(json_resource["identifier"], "20160901.json")
        self.assertEqual(json_resource["format"], "JSON")
        self.assertEqual(json_resource["media_type"], "application/json")
        self.assertEqual(json_resource["mimetype"], "application/json")
        # mimetype_inner is None, so CKAN doesn't save it on the resource
        self.assertNotIn("mimetype_inner", json_resource)

    def test_resource_formats_zip(self):
        currpath = os.path.dirname(os.path.realpath(__file__))
        src_path = os.path.join(currpath, "fixtures/zip/my.zip")
        dst_path = os.path.join(data.environment, data.folder, "20160901.zip")

        filesystem = self.get_filesystem()
        MockFTPStorageAdapter.filesystem = filesystem
        with open(src_path, "rb") as f:
            filesystem.writefile(path=dst_path, file=f)

        self.run_harvester(filter_regex=r".*\.zip", ftp_server="testserver")

        dataset = self.get_dataset()
        self.assertEqual(len(dataset["resources"]), 1)

        result = get_action("package_search")({}, {"facet.field": ["res_format"]})
        res_format_facet = result["facets"]["res_format"]
        self.assertDictEqual(res_format_facet, {"ZIP": 1})

        resource = dataset["resources"][0]

        self.assertEqual(resource["identifier"], "20160901.zip")
        self.assertEqual(resource["format"], "ZIP")
        self.assertEqual(resource["media_type"], "application/zip")
        self.assertEqual(resource["mimetype"], "application/zip")
        self.assertEqual(resource["mimetype_inner"], "text/plain")

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

    @time_machine.travel(
        datetime(2022, 4, 20, 14, 15, 0, 0, ZoneInfo("UTC")), tick=False
    )
    def test_datetime_fields(self):
        """Test that all datetime fields for a new dataset are set to the current time
        in UTC.
        """
        MockFTPStorageAdapter.filesystem = self.get_filesystem()
        self.run_harvester(ftp_server="testserver")

        dataset = self.get_dataset()

        dataset_datetime_fields = [
            "issued",
            "metadata_created",
            "metadata_modified",
            "modified",
            "version",
        ]
        resource_datetime_fields = [
            "created",
            "issued",
            "last_modified",
            "metadata_modified",
            "modified",
        ]

        for field in dataset_datetime_fields:
            self.assertEqual(dataset[field], "2022-04-20T14:15:00")

        for field in resource_datetime_fields:
            self.assertEqual(dataset["resources"][0][field], "2022-04-20T14:15:00")
