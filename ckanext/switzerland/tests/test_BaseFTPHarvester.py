# encoding: utf-8

'''Tests for the ckanext.switzerland.BaseFTPHarvester.py '''

import unittest

from nose.tools import assert_equal, raises
from mock import patch, Mock

import copy
import json
import os.path
import shutil
import ftplib

from pylons import config as ckanconf

from simplejson import JSONDecodeError

import logging
log = logging.getLogger(__name__)

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
# from ckanext.harvest.tests.lib import run_harvest

import ckanext.harvest.model as harvest_model

# not needed - mock ftplib instead
# from mock_ftp_server import MockFTPServer

from ckanext.switzerland.ftp.BaseFTPHarvester import BaseFTPHarvester
from ckanext.switzerland.ftp.BaseFTPHarvester import FTPHelper



class TestFTPHelper(unittest.TestCase):

    tmpfolder = '/tmp/ftpharvest/tests/'
    ftp = None

    @classmethod
    def setup_class(cls):

        # config for FTPServer as a tuple (host, port)
        # PORT = 1026 # http://stackoverflow.com/questions/24001147/python-bind-socket-error-errno-13-permission-denied
        # ftpconfig = ('127.0.0.1', PORT) # 990
        # mockuser = {
        #     'username': 'user',
        #     'password': '12345',
        #     'homedir': '.',
        #     'perm': 'elradfmw'
        # }
        # # Start FTP mock server
        # cls.ftp = MockFTPServer(ftpconfig, mockuser)

        pass

    @classmethod
    def teardown_class(cls):
        # close FTP server
        # if cls.ftp:
        #     cls.ftp.teardown()
        #     cls.ftp = None
        pass

    def setup(self):
        pass

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

    # FTP tests -----------------------------------------------------------

    @patch('ftplib.FTP_TLS', autospec=True)
    def test_ftp_connection(self, M):

        mockFTP_TLS = M.return_value

        # def mock_connect_fn(host, username, password):
        #     return "Connected"
        # MockFTP_TLS.prot_p = mock_connect_fn

        ftph = FTPHelper('/')
        ftph._connect()

        assert mockFTP_TLS.prot_p.called
        vars = {
            'host': ckanconf.get('ckan.ftp.host', ''),
            'username': ckanconf.get('ckan.ftp.username', ''),
            'password': ckanconf.get('ckan.ftp.password', ''),
        }
        assert mockFTP_TLS.prot_p.called_with(host, username, password)


    # @patch('ftplib.FTP_TLS', autospec=True)
    # def test_ftp_port_setting(self, mockFTP_TLS):

    #     mock_ftp = mockFTP_TLS.return_value

    #     def mock_connect_fn(host, username, password):
    #         return "Connected"
    #     mock_ftp.prot_p = mock_connect_fn

    #     ftph = FTPHelper('/')
    #     ftph._connect()





        # # port was set on ftplib library
        # assert_equal(990, int(ckanconf.get('ckan.ftp.port')))
        # assert_equal(int(MockFTP.port), int(ckanconf.get('ckan.ftp.port')))



    # TODO
    # @patch('ftplib.FTP_TLS', autospec=True)
    # def test_ftp_disconnect(self, MockFTP_TLS):

    #     ftph = FTPHelper('/')
    #     ftph._disconnect()

    #     assert MockFTP_TLS.quit.called # '221 Goodbye.'




    # @patch('open', autospec=True)






# =========================================================================

class TestBaseFTPHarvester(unittest.TestCase):

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

    def test__get_rest_api_offset(self):
        bh = BaseFTPHarvester()
        assert_equal(bh._get_rest_api_offset(), '/api/2/rest')
    def test__get_action_api_offset(self):
        bh = BaseFTPHarvester()
        assert_equal(bh._get_action_api_offset(), '/api/3/action')
    def test__get_search_api_offset(self):
        bh = BaseFTPHarvester()
        assert_equal(bh._get_search_api_offset(), '/api/2/search')

    def test_get_remote_folder(self):
        bh = BaseFTPHarvester()
        assert_equal(bh.get_remote_folder(), '/test/')

    def test_ckanapi_valid_connection(self):
        bh = BaseFTPHarvester()
        ckan = bh.ckanapi_connect('https://data.gov.uk', '<apikey>')
        assert_equal(str(type(ckan)), "<class 'ckanapi.remoteckan.RemoteCKAN'>")

    # TODO
    # def test_ckanapi_invalid_connection(self):
    #     bh = BaseFTPHarvester()
    #     ckan = bh.ckanapi_connect('http://foo', '<apikey>')
    #     assert_equal(ckan, False)

    def test_get_local_dirlist(self):
        bh = BaseFTPHarvester()
        dirlist = bh._get_local_dirlist(localpath="./ckanext/switzerland/tests/fixtures/testdir")
        assert_equal(type(dirlist), list)
        assert_equal(len(dirlist), 3)

    def test_set_config(self):
        bh = BaseFTPHarvester()
        bh._set_config('{"myvar":"test"}')
        assert_equal(bh.config['myvar'], "test")

    @raises(JSONDecodeError)
    def test_set_invalid_config(self):
        bh = BaseFTPHarvester()
        bh._set_config('{"myvar":test"}')
        assert_equal(bh.config['myvar'], "test")

    def test_set_invalid_config(self):
        bh = BaseFTPHarvester()
        bh._set_config(None)
        assert_equal(bh.config, {})
        bh._set_config('')
        assert_equal(bh.config, {})

    # TODO
    # def test_info(self):
    #     bh = BaseFTPHarvester()
    #     info = bh.info()
    #     assert_equal(info['name'], '')
    #     assert_equal(info['title'], '')
    #     assert_equal(info['description'], '')
    #     assert_equal(info['form_config_interface'], 'Text')

    def test_add_harvester_metadata(self):
        bh = BaseFTPHarvester()
        bh.package_dict_meta = {
            'foo': 'bar',
            'hello': 'world'
        }
        package_dict = {}
        context = {}
        package_dict = bh._add_harvester_metadata(package_dict, context)
        assert package_dict['foo']
        assert package_dict['hello']
        assert_equal(package_dict['foo'], 'bar')
        assert_equal(package_dict['hello'], 'world')

    def test_add_package_tags(self):
        bh = BaseFTPHarvester()

        package_dict = bh._add_package_tags({})
        assert_equal(package_dict['tags'], [])
        assert_equal(package_dict['num_tags'], 0)

        tags = ['a', 'b', 'c']

        bh = BaseFTPHarvester()
        bh.config = {
            'default_tags': tags
        }
        package_dict = bh._add_package_tags({})
        assert_equal(package_dict['tags'], tags)
        assert_equal(package_dict['num_tags'], 3)

    def test_add_package_groups(self):
        groups = ['groupA', 'groupB']
        bh = BaseFTPHarvester()
        bh.config = {
            'default_groups': groups
        }
        package_dict = bh._add_package_groups({})
        assert_equal(package_dict['groups'], groups)

    # TODO
    # def test_add_package_extras(self):
    #     package_dict = {
    #         'id': '123-456-789'
    #     }
    #     harvest_object = {
    #         'id': 'my-harvestobject-id',
    #         'job': {
    #             'id': 'jobid',
    #             'source':{
    #                 'id': 'harvester_id',
    #                 'title': 'MyHarvestObject',
    #                 'url': '/test/'
    #             }
    #         }
    #     }
    #     extras = {'hello':'world','foo':'bar'}
    #     bh = BaseFTPHarvester()
    #     bh.config = {
    #         'override_extras': extras
    #     }
    #     package_dict = bh._add_package_extras(package_dict, harvest_object)
    #     assert_equal(package_dict['extras']['foo'], extras['foo'])
    #     assert_equal(package_dict['extras']['hello'], extras['hello'])


    def test_convert_values_to_json_strings(self, resource_meta=None):
        test_package = {
            'foo': 'bar',
            'list': ['a', 'b', 'c'],
            'dict': {'a':'b'}
        }
        bh = BaseFTPHarvester()
        package_dict = bh._convert_values_to_json_strings(test_package)
        assert_equal(package_dict['foo'],  'bar')
        assert_equal(json.dumps(package_dict['list']), json.dumps(test_package['list']))
        assert_equal(json.dumps(package_dict['dict']), json.dumps(test_package['dict']))

    def test_remove_tmpfolder(self):
        tmpfolder = ''
        bh = BaseFTPHarvester()
        ret = bh.remove_tmpfolder(None)
        assert_equal(ret,  None)
        ret = bh.remove_tmpfolder('')
        assert_equal(ret,  None)

        tmpfolder = '/tmp/test_remove_tmpfolder'
        os.makedirs(tmpfolder, 0777)
        ret = bh.remove_tmpfolder(tmpfolder)
        assert not os.path.exists(tmpfolder)




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



