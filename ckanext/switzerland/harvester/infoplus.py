import csv
import json
from operator import itemgetter
from zipfile import ZipFile
import re
import os
import logging

import voluptuous
from ckanext.harvest.model import HarvestObject

log = logging.getLogger(__name__)


def get_validation_schema():
    def validate_infoplus(files):
        ret = {}
        for filename, config in files.items():
            voluptuous.Schema(basestring)(filename)
            voluptuous.Schema(dict)(config)
            ret[filename] = {}
            for col, name in config.items():
                voluptuous.Schema(basestring)(name)
                ret[filename][int(col)] = name
        return ret

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
    infoplus_config = {
        0: 'Didok Number',
        10: 'Latitude',
        20: 'Longtitude',
        30: 'Height',
        37: '<ignore>',  # ignore the % sign
        39: 'Station',
    }

    Output:
    Didok Number,Latitude,Longtitude,Height,Station
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
    fp.close()

    with open(path, 'w') as f:
        writer = csv.writer(f)

        infoplus_config = config['infoplus']['files'][harvester_obj['infoplus_filename']]

        config = sorted(infoplus_config.items(), key=itemgetter(0))  # order config items by position

        headings = map(itemgetter(1), filter(lambda k: k[1] != '<ignore>', config))
        writer.writerow(headings)

        # tuples of positions, e.g. [((0, 'Didok Number'), (10, 'Latitude')), ((10, 'Latitude'), (20, 'Longtitude'))]
        positions = list(zip(config, config[1:]))

        for line in data.split('\n'):
            row = []
            for (from_pos, column), (to_pos, _) in positions:
                if column == '<ignore>':
                    continue
                row.append(line[from_pos:to_pos].strip())

            # last position should go to the end of the file
            row.append(line[config[-1][0]:].strip())
            writer.writerow(row)

    zipfile.close()

    harvester_obj['file'] = path
    return harvester_obj
