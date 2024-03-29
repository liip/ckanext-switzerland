import json
import os
import shutil
import unittest
from datetime import datetime

import ckan.model as model
from ckan.lib import uploader
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.logic import get_action
from fs.memoryfs import MemoryFS

from ckanext.harvest import model as harvester_model
from ckanext.harvest.tests.factories import HarvestJobObj, HarvestSourceObj
from ckanext.harvest.tests.lib import run_harvest_job

from . import data


class BaseSBBHarvesterTests(unittest.TestCase):
    harvester_class = None

    def run_harvester(
        self,
        force_all=False,
        resource_regex=None,
        max_resources=None,
        dataset=data.dataset_name,
        timetable_regex=None,
        filter_regex=None,
        infoplus=None,
        ist_file=None,
        ftp_server=None,
    ):
        data.harvest_user()
        self.user = data.user()
        self.organization = data.organization(self.user)

        harvester = self.harvester_class()

        config = {
            "dataset": dataset,
            "environment": data.environment,
            "folder": data.folder,
        }
        if force_all:
            config["force_all"] = True
        if resource_regex:
            config["resource_regex"] = resource_regex
        if max_resources:
            config["max_resources"] = max_resources
        if timetable_regex:
            config["timetable_regex"] = timetable_regex
        if filter_regex:
            config["filter_regex"] = filter_regex
        if infoplus:
            config["infoplus"] = infoplus
        if ist_file:
            config["ist_file"] = ist_file
        if ftp_server:
            config["ftp_server"] = ftp_server

        source = HarvestSourceObj(
            url="http://example.com/harvest",
            config=json.dumps(config),
            source_type=harvester.info()["name"],
            owner_org=self.organization["id"],
        )

        job = HarvestJobObj(source=source, run=False)
        run_harvest_job(job, harvester)

        self.assertEqual(harvester_model.HarvestGatherError.count(), 0)
        self.assertEqual(harvester_model.HarvestObjectError.count(), 0)

    def get_dataset(self, name=data.dataset_name):
        return get_action("ogdch_dataset_by_identifier")({}, {"identifier": name})

    def get_package(self, name=data.dataset_name):
        return model.Package.get(self.get_dataset(name)["id"])

    def get_filesystem(self, filename=data.filename):
        fs = MemoryFS()
        fs.makedir(data.environment)
        fs.makedir(os.path.join(data.environment, data.folder))
        path = os.path.join(data.environment, data.folder, filename)
        fs.writetext(path, data.dataset_content_1)
        fs.settimes(path, modified=datetime(2000, 1, 1))
        return fs

    def assert_resource_data(self, resource_id, resource_data):
        resource_obj = model.Resource.get(resource_id)
        resource = resource_dictize(resource_obj, {"model": model})
        path = uploader.ResourceUpload(resource).get_path(resource_id)
        with open(path) as f:
            self.assertEqual(f.read(), resource_data)

    def assert_resource(self, resource_obj, exists):
        resource = resource_dictize(resource_obj, {"model": model})
        path = uploader.ResourceUpload(resource).get_path(resource_obj.id)
        self.assertEqual(os.path.exists(path), exists)

    def assert_resource_exists(self, resource_obj):
        self.assert_resource(resource_obj, True)

    def assert_resource_deleted(self, resource_obj):
        self.assert_resource(resource_obj, False)

    def _cleanup(self):
        if os.path.exists("/tmp/ckan_storage_path/"):
            shutil.rmtree("/tmp/ckan_storage_path/")

    def setUp(self):
        self._cleanup()

    def teardown(self):
        self._cleanup()
