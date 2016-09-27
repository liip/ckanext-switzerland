from ckanext.switzerland.harvester.timetable_harvester import TimetableHarvester
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
        self.run_harvester(dataset='Timetable {year}')

        dataset = self.get_dataset(name='Timetable 2016')

        assert_equal(len(dataset['resources']), 1)
