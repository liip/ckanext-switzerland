import shutil

import os
import json

from datetime import datetime

from ckan.lib.munge import munge_name
from mock import patch
from nose.tools import assert_equal, assert_raises

from ckan.logic import get_action, NotFound
from ckan.lib import search
import ckan.model as model
import ckan.lib.uploader as uploader

from ckanext.harvest.tests.factories import HarvestSourceObj, HarvestJobObj
from ckanext.harvest.tests.lib import run_harvest_job
from ckanext.harvest import model as harvester_model

from ckanext.switzerland.harvester.sbb_ftp_harvester import SBBFTPHarvester
from ckanext.switzerland.tests.helpers.mock_ftphelper import MockFTPHelper
from . import data


from fs.memoryfs import MemoryFS


@patch('ckanext.switzerland.harvester.sbb_ftp_harvester.FTPHelper', MockFTPHelper)
class TestSBBFTPHarvester(object):
    """
    Integration test for SBBFTPHarvester
    """
    def run_harvester(self, force_all=False):
        self.user = data.user()
        self.organization = data.organization(self.user)

        harvester = SBBFTPHarvester()

        config = {
            'dataset': data.dataset_name,
            'environment': data.environment,
            'folder': data.folder,
        }
        if force_all:
            config['force_all'] = True

        source = HarvestSourceObj(url='http://example.com/harvest', config=json.dumps(config),
                                  source_type=harvester.info()['name'], owner_org=self.organization['id'])

        job = HarvestJobObj(source=source, run=False)
        run_harvest_job(job, harvester)

    def get_dataset(self, name):
        return get_action('ogdch_dataset_by_identifier')({}, {'identifier': name})

    def get_package(self, name):
        return model.Package.get(self.get_dataset(name)['id'])

    def get_filesystem(self, filename=data.filename):
        fs = MemoryFS()
        fs.makedir(data.environment)
        fs.makedir(os.path.join(data.environment, data.folder))
        path = os.path.join(data.environment, data.folder, filename)
        fs.setcontents(path, data.dataset_content_1)
        fs.settimes(path, modified_time=datetime(2000, 1, 1))
        return fs

    def assert_resource_data(self, resource_id, resource_data):
        resource = get_action('resource_show')({}, {'id': resource_id})
        path = uploader.ResourceUpload(resource).get_path(resource_id)
        with open(path) as f:
            assert_equal(f.read(), resource_data)

    def test_simple(self):
        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset = self.get_dataset(data.dataset_name)

        assert_equal(len(dataset['resources']), 1)
        assert_equal(dataset['resources'][0]['identifier'], data.filename)

    def test_existing_dataset(self):
        data.dataset(slug='testslug-other-than-munge-name')

        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset1 = self.get_dataset(data.dataset_name)
        dataset2 = get_action('package_show')({}, {'id': 'testslug-other-than-munge-name'})

        assert_equal(dataset1['id'], dataset2['id'])
        with assert_raises(NotFound):
            get_action('package_show')({}, {'id': munge_name(data.dataset_name)})

    def test_existing_resource(self):
        """
        Tests harvesting a new file which was not harvested before. Should create a new resource
        and copy some data from the existing one.
        """
        dataset = data.dataset()
        data.resource(dataset=dataset)

        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset = self.get_dataset(data.dataset_name)

        assert_equal(len(dataset['resources']), 2)
        r1 = dataset['resources'][0]
        r2 = dataset['resources'][1]

        # resources are sorted in descending order
        assert_equal(r1['title']['de'], data.filename)  # the new resource gets a new name
        assert_equal(r2['title']['de'], 'AAAResource')

        # the new resource copies the description from the existing resource
        assert_equal(r1['description']['de'], 'AAAResource Desc')
        assert_equal(r2['description']['de'], 'AAAResource Desc')

    def test_existing_resource_same_filename(self):
        """
        Tests harvesting a new file which was not harvested before but manually uploaded to ckan.
        Should copy the data from the old resource and delete the old resource.
        """
        dataset = data.dataset()
        data.resource(dataset=dataset, filename=data.filename)

        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester()

        dataset = self.get_dataset(data.dataset_name)

        assert_equal(len(dataset['resources']), 1)
        resource = dataset['resources'][0]

        assert_equal(resource['title']['de'], 'AAAResource')
        assert_equal(resource['description']['de'], 'AAAResource Desc')

    def test_skip_already_harvested_file(self):
        """
        When modified date of file is older than the last harvester run date, the file should not be harvested again
        """
        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester()
        self.run_harvester()

        assert_equal(harvester_model.HarvestSource.count(), 1)
        assert_equal(harvester_model.HarvestJob.count(), 2)

        package = self.get_package(data.dataset_name)

        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 1)

    def test_force_all(self):
        """
        When modified date of file is older than the last harvester run date, the file should not be harvested again
        force_all overrides this mechanism and reharvests all files on the ftp server.
        """
        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester(force_all=True)
        self.run_harvester(force_all=True)

        assert_equal(harvester_model.HarvestSource.count(), 1)
        assert_equal(harvester_model.HarvestJob.count(), 2)

        package = self.get_package(data.dataset_name)

        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 2)

    def test_updated_file_before_last_harvester_run(self):
        """
        When modified date of file is older than the last harvester run date, the file should not be harvested again,
        except when the file is missing in the dataset, that is what we are testing here.
        """
        filesystem = self.get_filesystem()
        MockFTPHelper.filesystem = filesystem
        self.run_harvester()

        path = os.path.join(data.environment, data.folder, 'NewFile')
        filesystem.setcontents(path, data.dataset_content_1)
        filesystem.settimes(path, modified_time=datetime(2000, 1, 1))
        self.run_harvester()

        dataset = self.get_dataset(data.dataset_name)

        assert_equal(len(dataset['resources']), 2)

    def test_update_version(self):
        filesystem = self.get_filesystem(filename='20160901.csv')
        MockFTPHelper.filesystem = filesystem
        self.run_harvester()

        package = self.get_package(data.dataset_name)
        assert_equal(len(package.resources), 1)
        assert_equal(len(package.resources_all), 1)

        path = os.path.join(data.environment, data.folder, '20160902.csv')
        filesystem.setcontents(path, data.dataset_content_2)

        self.run_harvester()

        package = self.get_package(data.dataset_name)

        # none of the resources should be deleted
        assert_equal(len(package.resources), 2)
        assert_equal(len(package.resources_all), 2)

        # order should be: newest file first
        assert_equal(package.resources[0].extras['identifier'], '20160902.csv')
        assert_equal(package.resources[1].extras['identifier'], '20160901.csv')

        # permalink
        assert_equal(package.permalink, 'http://ogdch.dev/dataset/{}/resource/{}/download/20160902.csv'.format(
            package.id, package.resources[0].id))

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
        filesystem = self.get_filesystem(filename='20160901.csv')
        MockFTPHelper.filesystem = filesystem
        path = os.path.join(data.environment, data.folder, '20160902.csv')
        filesystem.setcontents(path, data.dataset_content_2)
        self.run_harvester()

        path = os.path.join(data.environment, data.folder, '20160901.csv')
        filesystem.setcontents(path, data.dataset_content_3)
        filesystem.settimes(path, modified_time=datetime.now())

        self.run_harvester()

        package = self.get_package(data.dataset_name)

        # there should be 3 resources now, 1 of them deleted
        assert_equal(len(package.resources), 2)
        assert_equal(len(package.resources_all), 3)

        # order should be: newest file first
        assert_equal(package.resources[0].extras['identifier'], '20160902.csv')
        assert_equal(package.resources[1].extras['identifier'], '20160901.csv')

        assert_equal(package.permalink, 'http://ogdch.dev/dataset/{}/resource/{}/download/20160902.csv'.format(
            package.id, package.resources[0].id))

        self.assert_resource_data(package.resources[0].id, data.dataset_content_2)
        self.assert_resource_data(package.resources[1].id, data.dataset_content_3)

    def test_update_revision(self):
        # test if resource was deleted, but file and datastore table still exists
        pass

    def test_update_version_regex(self):
        pass

    def test_update_revision_regex(self):
        pass

    # cleanup tests
    def test_max_resources(self):
        pass

    def test_max_revisions(self):
        pass

    def _cleanup(self):
        model.repo.rebuild_db()  # clear database
        search.clear_all()  # clear solr search index
        if os.path.exists('/tmp/ckan_storage_path/'):
            shutil.rmtree('/tmp/ckan_storage_path/')

    def setUp(self):
        self._cleanup()

    def teardown(self):
        self._cleanup()
