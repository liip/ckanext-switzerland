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

from ckanext.harvest.tests.factories import HarvestSourceObj, HarvestJobObj
from ckanext.harvest.tests.lib import run_harvest_job

from ckanext.switzerland.harvester.sbb_ftp_harvester import SBBFTPHarvester
from ckanext.switzerland.tests.helpers.mock_ftphelper import MockFTPHelper
from . import data

from fs.memoryfs import MemoryFS


@patch('ckanext.switzerland.harvester.sbb_ftp_harvester.FTPHelper', MockFTPHelper)
class TestSBBFTPHarvester(object):
    """
    Integration test for SBBFTPHarvester
    """
    def run_harvester(self):
        self.user = data.user()
        self.organization = data.organization(self.user)

        harvester = SBBFTPHarvester()

        source = HarvestSourceObj(url='http://example.com/harvest', config=json.dumps({
            'dataset': data.dataset_name,
            'environment': data.environment,
            'folder': data.folder,
        }), source_type=harvester.info()['name'], owner_org=self.organization['id'])

        job = HarvestJobObj(source=source, run=False)
        run_harvest_job(job, harvester)

    def get_dataset(self, name):
        return get_action('ogdch_dataset_by_identifier')({}, {'identifier': name})

    def get_filesystem(self):
        fs = MemoryFS()
        fs.makedir(data.environment)
        fs.makedir(os.path.join(data.environment, data.folder))
        path = os.path.join(data.environment, data.folder, data.filename)
        fs.setcontents(path, data.dataset_data)
        fs.settimes(path, modified_time=datetime(2000, 1, 1))
        return fs

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



    """
    def test_existing_resource(self):
        dataset = data.dataset()
        data.resource(dataset=dataset)
        factories.Resource(dataset=dataset['id'], identifier='AAAResource',
                           title={'de': 'AAAResource', 'en': 'AAAResource', 'fr': 'AAAResource',
                                  'it': 'AAAResource'},
                           description={'de': 'AAAResource Desc', 'en': 'AAAResource Desc',
                                        'fr': 'AAAResource Desc', 'it': 'AAAResource Desc'})

        dataset = self.get_dataset(data.dataset_name)

        assert_equal(len(dataset['resources']), 2)
        r1 = dataset['resources'][0]
        r2 = dataset['resources'][1]

        assert_equal(r1['title']['de'], 'AAAResource')
        assert_equal(r2['title']['de'], data.filename)
        assert_equal(r1['description']['de'], 'AAAResource Desc')
        assert_equal(r2['description']['de'], 'AAAResource Desc')
    """

    def test_update_revision(self):
        pass

    def test_update_version(self):
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
