import json
from zipfile import ZipFile

import os
from StringIO import StringIO

from ckanext.switzerland.harvester.timetable_harvester import TimetableHarvester
from ckanext.switzerland.tests import data
from ckanext.switzerland.tests.helpers.mock_ftphelper import MockFTPHelper
from mock import patch
from nose.tools import assert_equal, assert_is_not_none, assert_in

from .base_ftp_harvester_tests import BaseFTPHarvesterTests


@patch('ckanext.switzerland.harvester.timetable_harvester.FTPHelper', MockFTPHelper)
@patch('ckanext.switzerland.harvester.base_ftp_harvester.FTPHelper', MockFTPHelper)
class TestInfoplusHarvester(BaseFTPHarvesterTests):
    """
    Integration test for TimetableHarvester with Infoplus files
    """

    harvester_class = TimetableHarvester

    def test_config(self):
        MockFTPHelper.filesystem = self.get_filesystem()
        harvester = TimetableHarvester()
        harvester.validate_config(json.dumps({
            'dataset': data.dataset_name,
            'environment': data.environment,
            'folder': data.folder,
            'timetable_regex': 'FP(\d{4}).*\.zip',
            'infoplus': {
                'year': 2015,
                'dataset': 'Station List',
                'files': {
                    'BAHNHOF': data.infoplus_config
                }
            },
        }))

    def test_simple(self):
        filesystem = self.get_filesystem(filename='FP2016_Jahresfahrplan.zip')
        MockFTPHelper.filesystem = filesystem

        path = os.path.join(data.environment, data.folder, 'FP2015_Jahresfahrplan.zip')
        filesystem.setcontents(path, data.dataset_content_1)

        path = os.path.join(data.environment, data.folder, 'FP2015_Fahrplan_20150901.zip')
        filesystem.setcontents(path, data.dataset_content_1)

        path = os.path.join(data.environment, data.folder, 'FP2015_Fahrplan_20151001.zip')
        f = StringIO()
        zipfile = ZipFile(f, 'w')
        zipfile.writestr('BAHNHOF', data.bahnhof_file)
        zipfile.close()
        filesystem.setcontents(path, f.getvalue())

        self.run_harvester(
            dataset='Timetable {year}',
            timetable_regex='FP(\d{4}).*\.zip',
            resource_regex='FP(\d{4})_Fahrplan_\d{8}\.zip',
            infoplus={
                'year': 2015,
                'dataset': 'Station List',
                'files': {
                    'BAHNHOF': data.infoplus_config
                }
            }
        )

        dataset = self.get_dataset(name='Station List')

        assert_equal(len(dataset['resources']), 1)

        assert_in('issued', dataset)
        assert_in('modified', dataset)
        assert_is_not_none(dataset['issued'])
        assert_is_not_none(dataset['modified'])

        assert_equal(dataset['resources'][0]['identifier'], 'BAHNHOF.csv')
        self.assert_resource_data(dataset['resources'][0]['id'], data.bahnhof_file_csv)
