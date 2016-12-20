import json
from zipfile import ZipFile
import re
import os
import logging

import unicodecsv
import voluptuous
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)


def get_validation_schema():
    column_schema = voluptuous.Schema({
        'from': int,
        'to': int,
        'name': basestring,
    })

    def validate_infoplus(files):
        for filename, config in files.items():
            voluptuous.Schema(basestring)(filename)
            voluptuous.Schema(list)(config)
            for column in config:
                column_schema(column)
        return files

    return voluptuous.Schema({
        voluptuous.Required('files'): validate_infoplus,
        voluptuous.Required('dataset'): basestring,
        voluptuous.Required('year'): int,
    })


def get_filename(filelist_with_dataset, harvester_config):
    """
    Get the file from which we should extract and convert the infoplus files
    """
    files = []
    for filename, dataset in filelist_with_dataset:
        year = int(re.match(harvester_config['timetable_regex'], filename).group(1))
        if year == harvester_config['infoplus']['year']:
            if re.match(harvester_config['resource_regex'], filename):
                files.append(filename)

    if not files:
        return None

    files.sort(reverse=True)
    return files[0]


def create_harvest_jobs(harvester_config, harvester_name, harvest_job, zip_filename, workingdir):
    job_ids = []

    for filename in harvester_config['infoplus']['files'].keys():
        obj = HarvestObject(guid=harvester_name, job=harvest_job)
        # serialise and store the dirlist
        obj.content = json.dumps({
            'type': 'file-skip-download',
            'file': zip_filename,
            'tmpfolder': workingdir,
            'dataset': harvester_config['infoplus']['dataset'],
            'infoplus_filename': filename,
            'filter': 'infoplus',
        })
        # save it for the next step
        obj.save()
        job_ids.append(obj.id)

    obj = HarvestObject(guid=harvester_name, job=harvest_job)
    obj.content = json.dumps({'type': 'finalizer', 'dataset': harvester_config['infoplus']['dataset']})
    obj.save()
    job_ids.append(obj.id)

    return job_ids


def file_filter(harvester_obj, config):
    """
    Convert a fixed width textfile to csv.
    Example file:
    0000006   7.549783  47.216111 441    % St. Katharinen
    0000007   9.733756  46.922368 744    % Fideris
    0000011   7.389462  47.191804 467    % Grenchen Nord
    0000016   6.513937  46.659019 499    % La Sarraz, Couronne

    Matching config['infoplus']['files'] configuration:
    infoplus_config = [
        {'from': 1, 'to': 7, 'name': 'StationID'},
        {'from': 8, 'to': 18, 'name': 'Longitude'},
        {'from': 20, 'to': 29, 'name': 'Latitude'},
        {'from': 31, 'to': 36, 'name': 'Height'},
        {'from': 40, 'to': -1, 'name': 'Remark'},
    ]

    Output:
    StationID,Longitude,Latitude,Height,Remark
    0000006,7.549783,47.216111,441,St. Katharine
    0000007,9.733756,46.922368,744,Fideri
    0000011,7.389462,47.191804,467,Grenchen Nor
    0000016,6.513937,46.659019,499,"La Sarraz, Couronn"
    """
    log.info('Extracting file {} from Info+ zip file'.format(harvester_obj['infoplus_filename']))

    zipfile = ZipFile(os.path.join(harvester_obj['tmpfolder'], harvester_obj['file']), 'r')
    path = os.path.join(harvester_obj['tmpfolder'], harvester_obj['infoplus_filename'] + '.csv')

    fp = zipfile.open(harvester_obj['infoplus_filename'])
    data = fp.read()
    data = data.decode('iso-8859-1')
    fp.close()

    with open(path, 'wb') as f:
        writer = unicodecsv.writer(f, encoding='utf-8')

        infoplus_config = config['infoplus']['files'][harvester_obj['infoplus_filename']]

        headings = map(lambda col: col['name'], infoplus_config)
        writer.writerow(headings)

        for line in data.split('\n'):
            row = []
            for config in infoplus_config:
                from_pos = config['from'] - 1
                to_pos = config['to'] if config['to'] != -1 else len(line)
                row.append(line[from_pos:to_pos].strip())
            writer.writerow(row)

    zipfile.close()

    harvester_obj['file'] = path
    return harvester_obj
