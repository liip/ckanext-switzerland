# encoding: utf-8

'''Tests for the ckanext.switzerland.BaseFTPHarvester.py '''

import unittest

import copy
import json
import os.path
import shutil
import ftplib

import logging
log = logging.getLogger(__name__)

from nose.tools import assert_equal, raises, nottest, with_setup
from mock import patch, Mock
from mock import MagicMock
from mock import PropertyMock
from testfixtures import Replace

from pylons import config as ckanconf

from simplejson import JSONDecodeError

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


# TODO: Start ftp server we can test harvesting against it
from helpers.mock_ftp_server import MockFTPServer



# The classes to test
# -----------------------------------------------------------------------
from ckanext.switzerland.ftp.BaseFTPHarvester import BaseFTPHarvester
from ckanext.switzerland.ftp.harvesters import InfoplusHarvester
from ckanext.switzerland.ftp.harvesters import DidokHarvester
# -----------------------------------------------------------------------



class HarvestSource():
    id = 'harvester_id'
    title = 'MyHarvestObject'
    url = '/test/'
    def __init__(self, url=None):
        if url:
            self.url = url
class HarvestJob():
    _sa_instance_state = 'running'
    def __init__(self, id=None, source=None):
        if not id:
            self.id = 'jobid'
        else:
            self.id = id
        if not source:
            self.source = HarvestSource()
        else:
            self.source = source
class HarvestObject():
    id = 'my-harvest-object-id'
    url = 'my-url'
    guid = '1234-5678-6789'
    job__source__owner_org = 'g3298-23hg782-g8743g-348934'
    def __init__(self, guid=None, job=None, content=None, job__source__owner_org=None):
        if guid:
            self.guid = guid
        if content:
            self.content = content
        if job__source__owner_org:
            self.job__source__owner_org = job__source__owner_org
        if not job:
            self.job = HarvestJob()
        else:
            self.job = job
    def get(self, id):
        return self


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
        harvest_model.setup()
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

    @nottest
    def prereqs(self):
        # cleanup before testing
        shutil.rmtree('/tmp/mytestfolder', ignore_errors=True)
    @nottest
    def outro(self):
        # cleanup after testing
        shutil.rmtree('/tmp/mytestfolder', ignore_errors=True)

    # -------------------------------------------------------------------------------
    # BEGIN UNIT tests ----------------------------------------------------------------
    # -------------------------------------------------------------------------------

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

    def test_set_config(self):
        bh = BaseFTPHarvester()
        bh._set_config('{"myvar":"test"}')
        assert_equal(bh.config['myvar'], "test")

    @raises(JSONDecodeError)
    def test_set_invalid_config(self):
        bh = BaseFTPHarvester()
        bh._set_config('{"myvar":invalidjson"}')

    def test_set_config(self):
        bh = BaseFTPHarvester()
        bh._set_config(None)
        assert_equal(bh.config, {})
        bh._set_config('')
        assert_equal(bh.config, {})

    def test_info_defaults(self):
        bh = BaseFTPHarvester()
        info = bh.info()
        assert_equal(info['name'], 'ckanftpharvest')
        assert_equal(info['title'], 'CKAN FTP ckanftp Harvester')
        assert_equal(info['description'], 'Fetches %s' % '/test/')
        assert_equal(info['form_config_interface'], 'Text')

    def test_info_instantiated(self):
        class MyHarvester(BaseFTPHarvester):
            harvester_name = 'InfoPlus'
            def get_remote_folder(self):
                return '/my/folder/'
        harvester = MyHarvester()
        info = harvester.info()
        assert_equal(info['name'], 'infoplusharvest')
        assert_equal(info['title'], 'CKAN FTP InfoPlus Harvester')
        assert_equal(info['description'], 'Fetches %s' % '/my/folder/')
        assert_equal(info['form_config_interface'], 'Text')

    def test_validate_config_nil(self):
        bh = BaseFTPHarvester()
        conf = bh.validate_config(None)
        assert_equal(conf, None)
        conf = bh.validate_config({})
        assert_equal(conf, {})

    @raises(TypeError)
    def test_validate_config_invalid_json(self):
        config = {
            'invalid': 'conf'
        }
        bh = BaseFTPHarvester()
        conf = bh.validate_config(config)

    @raises(ValueError)
    def test_validate_config_invalid_lists(self):
        config = json.dumps({
            'default_tags': 'tag1,tag2',
            'default_groups': 'group1,group2'
        })
        bh = BaseFTPHarvester()
        conf = bh.validate_config(config)

    @raises(ValueError)
    def test_validate_config_invalid_lists(self):
        config = json.dumps({
            'default_tags': 'tag1,tag2'
        })
        bh = BaseFTPHarvester()
        conf = bh.validate_config(config)

    # TODO
    # @patch('ckan.model', autospec=True)
    # @patch('ckan.lib.base.c', autospec=True)
    # def test_validate_valid_config(self, c, model):
    #     c = {}
    #     model = {}
    #     config = json.dumps({
    #         'api_version': 3,
    #         'default_tags': ['tag1','tag2'],
    #         'default_groups': ['group1','group2'],
    #         'default_extras': {},
    #         'user': 'default',
    #         'read_only': True,
    #         'force_all': False
    #     })
    #     bh = BaseFTPHarvester()
    #     conf = bh.validate_config(config)

    def test_add_harvester_metadata(self):
        bh = BaseFTPHarvester()
        bh.package_dict_meta = {
            'foo': 'bar',
            'hello': 'world'
        }
        context = {}
        package_dict = bh._add_harvester_metadata({}, context)
        assert package_dict['foo']
        assert package_dict['hello']
        assert_equal(package_dict['foo'], 'bar')
        assert_equal(package_dict['hello'], 'world')

    def test_add_package_tags(self):
        bh = BaseFTPHarvester()
        context = {}
        package_dict = bh._add_package_tags({}, context)
        assert_equal(package_dict['tags'], [])
        assert_equal(package_dict['num_tags'], 0)

        tags = ['a', 'b', 'c']

        bh = BaseFTPHarvester()
        bh.config = {
            'default_tags': tags
        }
        package_dict = bh._add_package_tags({}, context)
        assert_equal(package_dict['tags'], tags)
        assert_equal(package_dict['num_tags'], 3)

    def group_getaction(self):
        # assume that group exists and is returned
        return {
            'id': '123-456-789',
            'name': 'My Existing Group'
        }
    @patch('ckanext.switzerland.ftp.BaseFTPHarvester.get_action', spec=group_getaction)
    def test_add_package_groups(self, get_action):
        context = {}
        bh = BaseFTPHarvester()
        # 1
        package_dict = bh._add_package_groups({}, context)
        assert_equal(package_dict['groups'], [])
        # 2
        bh.config = {
            'default_groups': [ 'mygroup', 'nothergroup' ]
        }
        package_dict = bh._add_package_groups({}, context)
        log.debug(package_dict)
        assert_equal(len(package_dict['groups']), 2)

    # class org_conf():
    #     def get(self, key, na):
    #         return 'the-configured-org-id'
    # def org_getaction(self, action):
    #     def mock_func(context, search_dict):
    #         # assume that group exists and is returned
    #         return {
    #             'id': search_dict['id'],
    #             'name': 'My Existing Org'
    #         }
    #     return mock_func
    # @patch('ckanext.switzerland.ftp.BaseFTPHarvester.ckanconf', spec=org_conf)
    # @patch('ckanext.switzerland.ftp.BaseFTPHarvester.get_action', spec=org_getaction)
    # def test_add_package_orgs(self, get_action, org_conf):
    #     context = {}
    #     # 1
    #     bh = BaseFTPHarvester()
    #     package_dict = bh._add_package_orgs({}, context)
    #     assert_equal(package_dict['owner_org'], 'the-configured-org-id')
    #     assert_equal(package_dict['organization']['id'], 'the-configured-org-id')
    #     assert_equal(package_dict['organization']['name'], 'My Existing Org')

    def test_add_package_extras(self):
        package_dict = {
            'id': '123-456-789'
        }
        harvest_object = HarvestObject()
        bh = BaseFTPHarvester()
        extras = {'hello':'world','foo':'bar'}
        # fake a config given in the web interface
        bh.config = {
            'override_extras': True,
            'default_extras': extras
        }
        package_dict = bh._add_package_extras(package_dict, harvest_object)
        assert_equal(package_dict['extras']['foo'], extras['foo'])
        assert_equal(package_dict['extras']['hello'], extras['hello'])

    def test_remove_tmpfolder(self):
        tmpfolder = ''
        bh = BaseFTPHarvester()
        ret = bh.remove_tmpfolder(None)
        # ---
        assert_equal(ret,  None)
        ret = bh.remove_tmpfolder('')
        assert_equal(ret,  None)
        # ---
        tmpfolder = '/tmp/test_remove_tmpfolder'
        os.makedirs(tmpfolder, 0777)
        ret = bh.remove_tmpfolder(tmpfolder)
        assert not os.path.exists(tmpfolder)

    def test_cleanup_after_error(self):
        tmpfolder = '/tmp/test_remove_tmpfolder'
        os.makedirs(tmpfolder, 0777)
        bh = BaseFTPHarvester()
        # ---
        bh.cleanup_after_error(None)
        # ---
        retobj = {
            'tmpfolder': tmpfolder
        }
        bh.cleanup_after_error(retobj)
        assert not os.path.exists(tmpfolder)

    def test_find_resource_in_package(self):
        dataset = {
            'resources': [
                {'id': '123-456-789', 'url':'http://example.com/my/url/123.txt', 'status':'active'},
                {'id': '123-456-789', 'url':'http://example.com/my/url/456.txt', 'status':'active'},
                {'id': '123-456-789', 'url':'http://example.com/my/url/789.txt', 'status':'deleted'}
            ]
        }
        harvest_object = HarvestObject()
        # test
        bh = BaseFTPHarvester()
        # 1
        filepath = '/tmp/ftpharvest/subdir/456.txt'
        ret = bh.find_resource_in_package(dataset, filepath, harvest_object)
        assert_equal(ret, dataset['resources'][1])
        # 2
        filepath = '/tmp/ftpharvest/subdir/789.txt'
        ret = bh.find_resource_in_package(dataset, filepath, harvest_object)
        # assert_equal(ret, None)
        assert_equal(ret, dataset['resources'][2])


    # ------------


    # def test_gather_normal(self):

    #     user_dict = {'user': 'testuser', 'pass': 'testpass', 'folder': '/'}
    #     mock_ftp = MockFTPServer(config=('127.0.0.1', 21), user=user_dict)

    #     # the source url is never used in the harvesters
    #     source = HarvestSourceObj(url='http://localhost/')
    #     job = HarvestJobObj(source=source)
    #     # run test
    #     harvester = DidokHarvester()
    #     obj_ids = harvester.gather_stage(job)
    #     # check
    #     assert_equal(job.gather_errors, [])

    #     assert_equal(type(obj_ids), list)

    #     assert_equal(len(obj_ids), len(mock_ftp.DATASETS))

    #     harvest_object = harvest_model.HarvestObject.get(obj_ids[0])

    #     assert_equal(harvest_object.guid, mock_ftp.DATASETS[0]['id'])

    #     assert_equal(json.loads(harvest_object.content), mock_ftp.DATASETS[0])


    # -------------------------------------------------------------------------------
    # END UNIT tests ----------------------------------------------------------------
    # -------------------------------------------------------------------------------



    # -------------------------------------------------------------------------------
    # BEGIN INTEGRATION tests -------------------------------------------------------
    # -------------------------------------------------------------------------------

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



