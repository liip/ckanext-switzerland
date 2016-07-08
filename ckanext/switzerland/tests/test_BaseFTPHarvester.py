# encoding: utf-8

'''Tests for the ckanext.switzerland.BaseFTPHarvester.py '''

from nose.tools import assert_equal
from mock import patch

import copy
import json
import os.path
import shutil

try:
    from ckan.tests.helpers import reset_db
    from ckan.tests.factories import Organization
except ImportError:
    from ckan.new_tests.helpers import reset_db
    from ckan.new_tests.factories import Organization
from ckan import model
import ckan

from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
                                             HarvestObjectObj)

# TODO
from ckanext.harvest.tests.lib import run_harvest

import ckanext.harvest.model as harvest_model


# TODO
from mock_ftp_server import FTPServer

from ckanext.switzerland.ftp.BaseFTPHarvester import BaseFTPHarvester
from ckanext.switzerland.ftp.BaseFTPHarvester import FTPHelper



class TestFTPHelper(object):

    tmpfolder = '/tmp/ftpharvest/tests/'
    ftp = None

    @classmethod
    def setup_class(cls):

        # config for FTPServer as a tuble (host, port)
        ftpconfig = ('', 990)

        mockuser = {
            'username': 'user',
            'password': '12345',
            'homedir': '.',
            'perm': 'elradfmw'
        }

        # Start FTP mock server
        self.ftp = FTPServer(ftpconfig, user=mockuser)
        self.ftp.setupFTPServer()

    @classmethod
    def setup(self):
        pass

    @classmethod
    def teardown_class(cls):
        # Close FTP mock server
        self.ftp.teardownFTPServer()

    def teardown(self):
        model.repo.rebuild_db()
        # remove the tmp directory
        
        if os.path.exists(self.tmpfolder):
            shutil.rmtree(self.tmpfolder)

    def test_FTPHelper__init__(self):
        """ FTPHelper class correctly stores the ftp configuration from the ckan config """
        remotefolder = '/test/'
        ftph = FTPHelper(remotefolder)

        assert_equal(ftph._config['username'], 'TESTUSER')
        assert_equal(ftph._config['password'], 'TESTPASS')
        assert_equal(ftph._config['host'], 'ftp-secure.sbb.ch')
        assert_equal(ftph._config['port'], 990)
        assert_equal(ftph._config['remotedirectory'], '/')
        assert_equal(ftph._config['localpath'], '/tmp/ftpharvest/tests/')

        assert_equal(ftph.remotefolder, remotefolder.rstrip('/'))

        assert os.path.exists(ftph._config['localpath'])

    def test_get_top_folder(self):
        foldername = "ftp-secure.sbb.ch:990"
        ftph = FTPHelper('/test/')
        assert_equal(foldername, ftph.get_top_folder())

    def test_mkdir_p(self):
        ftph = FTPHelper('/test/')
        ftph._mkdir_p(self.tmpfolder)
        assert os.path.exists(self.tmpfolder)

    def test_create_local_dir(self):
        ftph = FTPHelper('/test/')
        ftph.create_local_dir(self.tmpfolder)
        assert os.path.exists(self.tmpfolder)


class TestBaseFTPHarvester(object):

    @classmethod
    def setup_class(cls):
        # Start FTP-alike server we can test harvesting against it
        # mock_ftp.serve()
        # load plugins
        # ckan.plugins.load('ckanext-harvest')
        # ckan.plugins.load('ftpharvester')
        pass

    @classmethod
    def setup(self):
        reset_db()
        # TODO
        # harvest_model.setup()
        pass

    @classmethod
    def teardown_class(cls):
        # unload plugins
        # ckan.plugins.unload('ckanext-harvest')
        # ckan.plugins.unload('ftpharvester')
        pass

    def teardown(self):
        # TODO
        pass

    # BEGIN UNIT tests ----------------------------------------------------------------

    # def test_gather_unit(self):
    #     source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
    #     job = HarvestJobObj(source=source)

    #     harvester = BaseFTPHarvester()
    #     obj_ids = harvester.gather_stage(job)

    #     assert_equal(type(obj_ids), list)
    #     assert_equal(len(obj_ids), 1)

    #     harvest_object = harvest_model.HarvestObject.get(obj_ids[0])

    #     assert_equal(harvest_object.guid, harvester.harvester_name)

    # def test_fetch_unit(self):

    #     source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)

    #     job = HarvestJobObj(source=source)

    #     harvest_object = HarvestObjectObj(guid=mock_ckan.DATASETS[0]['id'], job=job)

    #     harvester = CKANHarvester()
    #     result = harvester.fetch_stage(harvest_object)

    #     assert_equal(result, True)
    #     assert_equal(
    #         harvest_object.content,
    #         json.dumps(
    #             mock_ckan.convert_dataset_to_restful_form(
    #                 mock_ckan.DATASETS[0])))

    # def test_import_unit(self):
    #     org = Organization()
    #     harvest_object = HarvestObjectObj(
    #         guid=mock_ckan.DATASETS[0]['id'],
    #         content=json.dumps(mock_ckan.convert_dataset_to_restful_form(
    #                            mock_ckan.DATASETS[0])),
    #         job__source__owner_org=org['id'])

    #     harvester = CKANHarvester()
    #     result = harvester.import_stage(harvest_object)

    #     assert_equal(result, True)
    #     assert harvest_object.package_id
    #     dataset = model.Package.get(harvest_object.package_id)
    #     assert_equal(dataset.name, mock_ckan.DATASETS[0]['name'])

    # END UNIT tests ----------------------------------------------------------------


    # BEGIN INTEGRATION tests -------------------------------------------------------

    # def test_harvest(self):
    #     results_by_guid = run_harvest(
    #         url='http://localhost:%s/' % mock_ckan.PORT,
    #         harvester=CKANHarvester())

    #     result = results_by_guid['dataset1-id']
    #     assert_equal(result['state'], 'COMPLETE')
    #     assert_equal(result['report_status'], 'added')
    #     assert_equal(result['dataset']['name'], mock_ckan.DATASETS[0]['name'])
    #     assert_equal(result['errors'], [])

    #     result = results_by_guid[mock_ckan.DATASETS[1]['id']]
    #     assert_equal(result['state'], 'COMPLETE')
    #     assert_equal(result['report_status'], 'added')
    #     assert_equal(result['dataset']['name'], mock_ckan.DATASETS[1]['name'])
    #     assert_equal(result['errors'], [])

    # def test_harvest_twice(self):
    #     run_harvest(
    #         url='http://localhost:%s/' % mock_ckan.PORT,
    #         harvester=CKANHarvester())

    #     # change the modified date
    #     datasets = copy.deepcopy(mock_ckan.DATASETS)
    #     datasets[1]['metadata_modified'] = '2050-05-09T22:00:01.486366'
    #     with patch('ckanext.harvest.tests.harvesters.mock_ckan.DATASETS',
    #                datasets):
    #         results_by_guid = run_harvest(
    #             url='http://localhost:%s/' % mock_ckan.PORT,
    #             harvester=CKANHarvester())

    #     # updated the dataset which has revisions
    #     result = results_by_guid[mock_ckan.DATASETS[1]['name']]
    #     assert_equal(result['state'], 'COMPLETE')
    #     assert_equal(result['report_status'], 'updated')
    #     assert_equal(result['dataset']['name'], mock_ckan.DATASETS[1]['name'])
    #     assert_equal(result['errors'], [])

    #     # the other dataset is unchanged and not harvested
    #     assert mock_ckan.DATASETS[1]['name'] not in result

    # def test_harvest_invalid_tag(self):
    #     from nose.plugins.skip import SkipTest; raise SkipTest()
    #     results_by_guid = run_harvest(
    #         url='http://localhost:%s/invalid_tag' % mock_ckan.PORT,
    #         harvester=CKANHarvester())

    #     result = results_by_guid['dataset1-id']
    #     assert_equal(result['state'], 'COMPLETE')
    #     assert_equal(result['report_status'], 'added')
    #     assert_equal(result['dataset']['name'], mock_ckan.DATASETS[0]['name'])

    # def test_exclude_organizations(self):
    #     config = {'organizations_filter_exclude': ['org1-id']}
    #     results_by_guid = run_harvest(
    #         url='http://localhost:%s' % mock_ckan.PORT,
    #         harvester=CKANHarvester(),
    #         config=json.dumps(config))
    #     assert 'dataset1-id' not in results_by_guid
    #     assert mock_ckan.DATASETS[1]['id'] in results_by_guid

    # def test_include_organizations(self):
    #     config = {'organizations_filter_include': ['org1-id']}
    #     results_by_guid = run_harvest(
    #         url='http://localhost:%s' % mock_ckan.PORT,
    #         harvester=CKANHarvester(),
    #         config=json.dumps(config))
    #     assert 'dataset1-id' in results_by_guid
    #     assert mock_ckan.DATASETS[1]['id'] not in results_by_guid

    # def test_harvest_not_modified(self):
    #     run_harvest(
    #         url='http://localhost:%s/' % mock_ckan.PORT,
    #         harvester=CKANHarvester())

    #     results_by_guid = run_harvest(
    #         url='http://localhost:%s/' % mock_ckan.PORT,
    #         harvester=CKANHarvester())

    #     # The metadata_modified was the same for this dataset so the import
    #     # would have returned 'unchanged'
    #     result = results_by_guid[mock_ckan.DATASETS[1]['name']]
    #     assert_equal(result['state'], 'COMPLETE')
    #     assert_equal(result['report_status'], 'not modified')
    #     assert 'dataset' not in result
    #     assert_equal(result['errors'], [])

    # END INTEGRATION tests ---------------------------------------------------------
