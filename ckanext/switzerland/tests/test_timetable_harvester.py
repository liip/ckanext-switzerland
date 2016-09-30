import os

from ckanext.switzerland.harvester.timetable_harvester import TimetableHarvester
from ckanext.switzerland.tests import data
from ckanext.switzerland.tests.helpers.mock_ftphelper import MockFTPHelper
from mock import patch
from nose.tools import assert_equal

from .base_ftp_harvester_tests import BaseFTPHarvesterTests


@patch('ckanext.switzerland.harvester.timetable_harvester.FTPHelper', MockFTPHelper)
@patch('ckanext.switzerland.harvester.base_ftp_harvester.FTPHelper', MockFTPHelper)
class TestTimetableHarvester(BaseFTPHarvesterTests):
    """
    Integration test for SBBFTPHarvester
    """

    harvester_class = TimetableHarvester

    def test_simple(self):
        MockFTPHelper.filesystem = self.get_filesystem(filename='FP2016_Jahresfahrplan.zip')
        self.run_harvester(dataset='Timetable {year}', timetable_regex='FP(\d\d\d\d).*')

        dataset = self.get_dataset(name='Timetable 2016')

        assert_equal(len(dataset['resources']), 1)

    def test_multi_year(self):
        filesystem = self.get_filesystem(filename='FP2016_Jahresfahrplan.zip')
        MockFTPHelper.filesystem = filesystem
        self.run_harvester(dataset='Timetable {year}', timetable_regex='FP(\d\d\d\d).*')

        path = os.path.join(data.environment, data.folder, 'FP2015_Jahresfahrplan.zip')
        filesystem.setcontents(path, data.dataset_content_3)

        path = os.path.join(data.environment, data.folder, 'InvalidFile')
        filesystem.setcontents(path, data.dataset_content_2)

        dataset1 = self.get_dataset(name='Timetable 2016')
        assert_equal(len(dataset1['resources']), 1)
        self.assert_resource_data(dataset1['resources'][0]['id'], data.dataset_content_1)

        dataset2 = self.get_dataset(name='Timetable 2015')
        assert_equal(len(dataset2['resources']), 1)
        self.assert_resource_data(dataset1['resources'][0]['id'], data.dataset_content_3)
