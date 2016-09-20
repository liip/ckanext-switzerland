import os
import json
import unittest

from datetime import datetime
from mock import patch
from nose.tools import assert_equal

from ckan.logic import get_action
from ckan.lib import search
from ckan.tests import factories
import ckan.model as model

from ckanext.harvest.tests.factories import HarvestSourceObj, HarvestJobObj
from ckanext.harvest.tests.lib import run_harvest_job

from ckanext.switzerland.harvester.sbb_ftp_harvester import SBBFTPHarvester
from ckanext.switzerland.tests.helpers.mock_ftphelper import MockFTPHelper

from fs.memoryfs import MemoryFS


@patch('ckanext.switzerland.harvester.sbb_ftp_harvester.FTPHelper', MockFTPHelper)
class TestSBBFTPHarvester(unittest.TestCase):
    """
    Integration test for SBBFTPHarvester
    """
    environment = 'Test'
    folder = 'Dataset'
    filename = 'Didok.csv'
    dataset_name = 'Dataset'
    dataset_data = 'Year;data\n2013;1\n2014;2\n2015;3\n'

    def run_harvester(self):
        self.user = factories.User()
        self.owner_org = factories.Organization(
            users=[{'name': self.user['id'], 'capacity': 'admin'}],
            description={'de': '', 'it': '', 'fr': '', 'en': ''},
            title={'de': '', 'it': '', 'fr': '', 'en': ''}
        )

        harvester = SBBFTPHarvester()

        source = HarvestSourceObj(url='http://example.com/harvest', config=json.dumps({
            'dataset': self.dataset_name,
            'environment': self.environment,
            'folder': self.folder,
        }), source_type=harvester.info()['name'], owner_org=self.owner_org['id'])

        job = HarvestJobObj(source=source, run=False)
        run_harvest_job(job, harvester)

    def get_dataset(self, name):
        return get_action('ogdch_dataset_by_identifier')({}, {'identifier': name})

    def get_filesystem(self):
        fs = MemoryFS()
        fs.makedir(self.environment)
        fs.makedir(os.path.join(self.environment, self.folder))
        path = os.path.join(self.environment, self.folder, self.filename)
        fs.setcontents(path, self.dataset_data)
        fs.settimes(path, modified_time=datetime(2000, 1, 1))
        return fs

    def test_simple(self):
        MockFTPHelper.filesystem = self.get_filesystem()
        self.run_harvester()
        assert_equal(len(self.dataset['resources']), 1)
        # TODO: check filename
        # TODO: check file content

    def test_correct_organization(self):
        pass

    def test_existing_dataset(self):
        pass

    def setUp(self):
        model.repo.rebuild_db()  # clear database
        search.clear_all()  # clear solr search index

    def teardown(self):
        model.repo.rebuild_db()  # clear database
        search.clear_all()  # clear solr search index
