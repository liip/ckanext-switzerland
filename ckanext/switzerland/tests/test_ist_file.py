import os
from ckanext.switzerland.harvester.sbb_ftp_harvester import SBBFTPHarvester
from ckanext.switzerland.tests.helpers.mock_ftphelper import MockFTPHelper
from mock import patch
from nose.tools import assert_equal

from . import data
from .base_ftp_harvester_tests import BaseSBBHarvesterTests


@patch('ckanext.switzerland.harvester.sbb_ftp_harvester.FTPHelper', MockFTPHelper)
@patch('ckanext.switzerland.harvester.base_sbb_harvester.FTPHelper', MockFTPHelper)
class TestIstFileHarvester(BaseSBBHarvesterTests):
    """
    Integration test for SBBFTPHarvester with ist_file
    """

    harvester_class = SBBFTPHarvester

    def test_simple(self):
        filesystem = self.get_filesystem(filename='ist_file.csv')
        path = os.path.join(data.environment, data.folder, 'ist_file.csv')
        filesystem.setcontents(path, data.ist_file)
        MockFTPHelper.filesystem = filesystem

        self.run_harvester(ist_file=True)

        dataset = self.get_dataset()

        assert_equal(len(dataset['resources']), 1)

        assert_equal(dataset['resources'][0]['identifier'], 'ist_file.csv')
        self.assert_resource_data(dataset['resources'][0]['id'], data.ist_file_output)
