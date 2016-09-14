"""
CKAN FTP Harvester
==================

A Harvesting Job is performed in three phases.
1) the **gather** stage collects all the files that need to be fetched
from the harvest source. Errors occurring in this phase
(``HarvestGatherError``) are stored in the ``harvest_gather_error``
table.
2) the **fetch** stage retrieves the ``HarvestedObjects``
3) the **import** stage stores them in the database. Errors occurring in the second and third stages
(``HarvestObjectError``) are stored in the ``harvest_object_error`` table.
"""

# import traceback
import logging

from ckan import model
from ckan.model import Session
from ckan.logic import NotFound
from ckan.logic import get_action, check_access
from ckan.lib.helpers import json
from ckan.lib.munge import munge_filename
from ckan.lib import helpers
from ckanext.harvest.harvesters.base import HarvesterBase
from pylons import config as ckanconf

import os
import ftplib  # for errors only
import tempfile
import time
from datetime import datetime
import shutil
import requests

import subprocess

from ckanext.harvest.model import HarvestJob, HarvestObject

from simplejson.scanner import JSONDecodeError

from ftp_helper import FTPHelper


log = logging.getLogger(__name__)


class SBBFTPHarvester(HarvesterBase):
    """
    A FTP Harvester for the SBB ftp server. This is a generic harvester
    which can be configured for specif datasets using the ckan harvester webinterface.
    """

    config = None  # ckan harvester config, not ftp config

    api_version = 2
    action_api_version = 3

    # default harvester id, to be overwritten by child classes
    harvester_name = 'SBB FTP Harvester'

    # default remote directory to harvest, to be overwritten by child classes
    # e.g. infodoc or didok
    remotefolder = ''

    # if a resource is uploaded with a format, it will show a tag on the dataset, e.g. XML or TXT
    # the default setting is defined to be TXT for files with no extension
    default_format = 'TXT'
    default_mimetype = 'TXT'
    default_mimetype_inner = 'TXT'

    tmpfolder_prefix = "%d%m%Y-%H%M-"

    # default package metadata
    package_dict_meta = {
        # package privacy
        'private': False,
        'state': 'active',
        'isopen': False,
        # --------------------------------------------------------------------------
        # author and maintainer
        'author': "Author name",
        'author_email': "author@example.com",
        'maintainer': "Maintainer name",
        'maintainer_email': "maintainer@example.com",
        # license
        'license_id': "other-open",
        'license_title': "Other (Open)",
        'rights': "Other (Open)",
        # ckan multilang/switzerland custom required fields
        'coverage': "Coverage",
        'issued': "21.03.2015",
        # "modified": "21.03.2016",
        # "metadata_created": "2016-07-05T07:41:28.741265",
        # "metadata_modified": "2016-07-05T07:43:30.079030",
        # "url": "https://catalog.data.gov/",
        "spatial": "Spatial",
        "accrual_periodicity": "",
        # --------------------------------------------------------------------------
        "description": {
            "fr": "FR Description",
            "en": "EN Description",
            "de": "DE Description",
            "it": "IT Description"
        },

        "notes": {
            "fr": "",
            "en": "",
            "de": "",
            "it": ""
        },
        # --------------------------------------------------------------------------
        'groups': [],
        'tags': [],
        'extras': [],
        "language": ["en", "de", "fr", "it"],
        # relations
        "relations": [{}],
        "relationships_as_object": [],
        "relationships_as_subject": [],
        "see_alsos": [],
        "publishers": [{
            "label": "Publisher 1"
        }],
        # keywords
        'keywords': {
            "fr": [],
            "en": [],
            "de": [],
            "it": []
        },
        'contact_points': [{
            "name": "Contact Name",
            "email": "contact@example.com"
        }],
        "temporals": [{
            "start_date": "2014-03-21T00:00:00",
            "end_date": "2019-03-21T00:00:00"
        }],
    }

    resource_dict_meta = {
        'state': 'active',
        'rights': 'Other (Open)',
        'license': 'Other (Open)',
        'coverage': 'Coverage',
    }

    # tested
    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version

    # tested
    def _get_action_api_offset(self):
        return '/api/%d/action' % self.action_api_version

    # tested
    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version

    # tested
    def get_remote_folder(self):
        return os.path.join('/', self.config['environment'], self.config['folder'])

    # tested
    def info(self):
        """
        Return basic information about the harvester

        :returns: Dictionary with basic information about the harvester
        :rtype: dict
        """
        return {
            'name': '%sharvest' % self.harvester_name.lower(),
            'title': self.harvester_name,
            'description': 'Fetches data from the SBB FTP Server',
            'form_config_interface': 'Text'
        }

    def validate_config(self, config_str):
        """
        Validates the configuration that can be pasted into the harvester web interface

        :param config_str: Configuration (JSON-encoded string)
        :type config_str: str

        :returns: Configuration dictionary
        :rtype: dict
        """
        if not config_str:
            raise ValueError('Harvester Configuration is required')

        config_obj = json.loads(config_str)

        if 'force_all' in config_obj and not isinstance(config_obj['force_all'], bool):
            raise ValueError('force_all must be boolean')

        for key in ('environment', 'folder', 'dataset'):
            if key not in config_obj:
                raise ValueError('Configuration option {} if required'.format(key))
            if not isinstance(config_obj[key], basestring):
                raise ValueError('Configuration option {} must be a string'.format(key))
        return config_str

    # tested
    def _add_harvester_metadata(self, package_dict, context):
        """
        Adds the metadata stored in the harvester class

        :param package_dict: Package metadata
        :type package_dict: dict
        :param context: CKAN context
        :type context: dict

        :returns: Package dictionary
        :rtype: dict
        """

        # is there a package meta configuration in the harvester?
        if self.package_dict_meta:
            # add each key/value from the meta data of the harvester
            for key, val in self.package_dict_meta.iteritems():
                package_dict[key] = val

        return package_dict

    # tested
    def _add_package_tags(self, package_dict):
        """
        Create tags

        :param package_dict: Package metadata
        :type package_dict: dict

        :returns: Package dictionary
        :rtype: dict
        """
        if 'tags' not in package_dict:
            package_dict['tags'] = []

        # add the tags from the config object
        default_tags = self.config.get('default_tags', [])
        if default_tags:
            package_dict['tags'].extend([t for t in default_tags if t not in package_dict['tags']])

        package_dict['num_tags'] = len(package_dict['tags'])

        return package_dict

    # tested
    def _add_package_groups(self, package_dict, context):
        """
        Create (default) groups

        :param package_dict: Package metadata
        :type package_dict: dict
        :param context: CKAN context
        :type context: dict

        :returns: Package dictionary
        :rtype: dict
        """
        if 'groups' not in package_dict:
            package_dict['groups'] = []

        # Set default groups if needed
        default_groups = self.config.get('default_groups', [])

        # one might also enter just a string -> convert it to list
        if not isinstance(default_groups, list):
            default_groups = [default_groups]

        # check if groups exist locally, otherwise do not add them
        for group_name in default_groups:
            try:
                group = get_action('group_show')(context, {'id': group_name})
                if self.api_version == 1:
                    package_dict['groups'].append(group['name'])
                else:
                    package_dict['groups'].append(group['id'])
            except NotFound:
                log.info('Group %s is not available' % group_name)

        return package_dict

    def _add_package_orgs(self, package_dict, context, organization):
        """
        Fetch organization and set it on the package_dict
        
        :param package_dict: Package metadata
        :type package_dict: dict
        :param context: CKAN context
        :type context: dict

        :returns: Package dictionary
        :rtype: dict
        """

        # check if this organization exists
        org_dict = get_action('organization_show')(context, {'id': organization})
        if org_dict:
            package_dict['owner_org'] = organization
            package_dict['organization'] = org_dict

        return package_dict

    # tested
    def _add_package_extras(self, package_dict, harvest_object):
        """
        Create default organization(s)
        
        :param package_dict: Package metadata
        :type package_dict: dict
        :param harvest_object: Instance of the Harvester Object

        :returns: Package dictionary
        :rtype: dict
        """

        # Set default extras if needed
        default_extras = self.config.get('default_extras', {})
        if default_extras:
            override_extras = self.config.get('override_extras', False)
            if 'extras' not in package_dict:
                package_dict['extras'] = {}
            for key, value in default_extras.iteritems():
                if key not in package_dict['extras'] or override_extras:
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip('/'),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict['id'])

                    package_dict['extras'][key] = value

        return package_dict

    # tested
    def remove_tmpfolder(self, tmpfolder):
        """ Remove the tmp folder, if it exists """
        if not tmpfolder:
            return
        shutil.rmtree(tmpfolder)

    # tested
    def cleanup_after_error(self, retobj):
        """ Do some clean-up tasks """
        if retobj and 'tmpfolder' in retobj:
            self.remove_tmpfolder(retobj['tmpfolder'])

    # tested
    def find_resource_in_package(self, dataset, filepath):
        """
        Identify a resource in a package by its (munged) filename

        :param dataset: dataset dictionary
        :type dataset: dict
        :param filepath: Local path to the downloaded file
        :type filepath: str

        :returns: Package dictionary
        :rtype: dict
        """
        resource_meta = None
        if 'resources' in dataset and len(dataset['resources']):
            # Find resource in the existing packages resource list
            for res in dataset['resources']:
                # match the resource by its filename
                match_name = munge_filename(os.path.basename(filepath))
                if os.path.basename(res.get('url')) != match_name:
                    continue
                resource_meta = res
                # there should only be one file with the same name in each dataset, so we can break
                break
        return resource_meta

    # =======================================================================
    # GATHER Stage
    # =======================================================================

    def gather_stage(self, harvest_job):
        """
        Dummy stage that launches the next phase

        :param harvest_job: Harvester job

        :returns: List of HarvestObject ids that are processed in the next stage (fetch_stage)
        :rtype: list
        """
        log.info('=====================================================')
        log.info('In %s FTPHarvester gather_stage' % self.harvester_name)  # harvest_job.source.url

        # set harvester config
        self.config = json.loads(harvest_job.source.config)

        modified_dates = {}

        # get a listing of all files in the target directory

        remotefolder = self.get_remote_folder()
        log.info("Getting listing from remotefolder: %s" % remotefolder)

        try:
            with FTPHelper(remotefolder) as ftph:
                filelist = ftph.get_remote_filelist()
                log.debug("Remote dirlist: %s" % str(filelist))

                # get last-modified date of each file
                for f in filelist:
                    modified_dates[f] = ftph.get_modified_date(f)

                # store some config for the next step

                # ftplib stores retrieved files in a folder, e.g. 'ftp-secure.sbb.ch:990'
                ftplibfolder = ftph.get_top_folder()

                # set base directory of the tmp folder
                local_path = ftph.get_local_path()
                tmpdirbase = os.path.join(ftph.get_local_path(), ftplibfolder.strip('/'), remotefolder.lstrip('/'))
                tempfile.tempdir = tmpdirbase

                # the base tmp folder needs to be created for the tempfile library
                if not os.path.exists(tmpdirbase):
                    ftph.create_local_dir(tmpdirbase)

                # set prefix for tmp folder
                prefix = datetime.now().strftime(self.tmpfolder_prefix)
                # save the folder path where the files are to be downloaded
                # all parts following the first one must be relative paths

                workingdir = tempfile.mkdtemp(prefix=prefix)
                log.info("Created workingdir: %s" % workingdir)

        except ftplib.all_errors as e:
            self._save_gather_error('Error getting remote directory listing: %s' % str(e), harvest_job)
            return None

        if not len(filelist):
            self._save_gather_error('No files found in %s' % remotefolder, harvest_job)
            return None

        # create one harvest job for each resource in the package
        # -------------------------------------------------------------------------
        object_ids = []

        # ------------------------------------------------------
        # 1: only download the resources that have been modified
        # has there been a previous run and was it successful?
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source == harvest_job.source) \
            .filter(HarvestJob.gather_finished.isnot(None)) \
            .filter(HarvestJob.id != harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()
        if previous_job and not previous_job.gather_errors and previous_job.gather_started:
            # optional 'force_all' config setting can be used to always download all files
            if self.config and not self.config.get('force_all', False):
                # Request only the resources modified since last harvest job
                for f in filelist[:]:
                    modified_date = modified_dates.get(f)
                    if modified_date and modified_date < previous_job.gather_started:
                        # do not run the harvest for this file
                        filelist.remove(f)

                if not len(filelist):
                    log.info('No files have been updated on the ftp server since the last harvest job')
                    return []  # no files to harvest this time
        # ------------------------------------------------------

        # ------------------------------------------------------
        # 2: download all resources
        for f in filelist:
            obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
            # serialise and store the dirlist
            obj.content = json.dumps({
                'type': 'file',
                'file': f,
                'workingdir': workingdir,
                'remotefolder': remotefolder
            })
            # save it for the next step
            obj.save()
            object_ids.append(obj.id)

        # ------------------------------------------------------
        # 3: Add finalizer task to queue
        obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
        obj.content = json.dumps({'type': 'finalizer', 'tempdir': local_path})
        obj.save()
        object_ids.append(obj.id)

        # ------------------------------------------------------
        # send the jobs to the gather queue
        return object_ids

    # =======================================================================
    # FETCH Stage
    # =======================================================================

    def fetch_stage(self, harvest_object):
        """
        Fetching of resources. Runs once for each gathered resource.

        :param harvest_object: HarvesterObject instance

        :returns: Whether HarvestObject was saved or not
        :rtype: mixed
        """
        log.info('=====================================================')
        log.info('In %s FTPHarvester fetch_stage' % self.harvester_name)
        log.info('Running harvest job %s' % harvest_object.id)
        stage = 'Fetch'

        if not harvest_object or not harvest_object.content:
            log.error('No harvest object received')
            self._save_object_error('No harvest object received', harvest_object, stage)
            return None

        try:
            obj = json.loads(harvest_object.content)
        except JSONDecodeError as e:
            log.error('Invalid harvest object: %s', harvest_object.content)
            self._save_object_error('Unable to decode harvester info: %s' % str(e), harvest_object.content, stage)
            return None

        if obj['type'] != 'file':
            return True

        # the file to harvest from the previous step
        f = obj.get('file')
        if not f:
            self._save_object_error('No file to harvest: %s' % harvest_object.content, harvest_object, stage)
            return None

        # the folder where the file is to be stored
        tmpfolder = obj.get('workingdir')
        if not tmpfolder:
            self._save_object_error('No tmpfolder received from gather step: %s' % harvest_object.content,
                                    harvest_object, stage)
            return None

        # the folder where the file is to be stored
        remotefolder = obj.get('remotefolder')
        if not remotefolder:
            self._save_object_error('No remotefolder received from gather step: %s' % harvest_object.content,
                                    harvest_object, stage)
            return None

        log.info("Remote directory: %s", remotefolder)
        log.info("Local directory: %s", tmpfolder)

        try:

            with FTPHelper(remotefolder) as ftph:

                # fetch file via ftplib
                # -------------------------------------------------------------------
                # full path of the destination file
                targetfile = os.path.join(tmpfolder, f)

                log.info('Fetching file: %s' % str(f))

                start = time.time()
                status = ftph.fetch(f, targetfile)  # 226 Transfer complete
                elapsed = time.time() - start

                log.info("Fetched %s [%s] in %ds" % (f, str(status), elapsed))

                if status != '226 Transfer complete':
                    self._save_object_error('Download error for file %s: %s' % (f, str(status)), harvest_object, stage)
                    return None

        except ftplib.all_errors as e:
            self._save_object_error('Ftplib error: %s' % str(e), harvest_object, stage)
            self.cleanup_after_error(tmpfolder)
            return None

        except Exception as e:
            self._save_object_error('An error occurred: %s' % e, harvest_object, stage)
            self.cleanup_after_error(tmpfolder)
            return None

        # store the info for the next step
        retobj = {
            'type': 'file',
            'file': targetfile,
            'tmpfolder': tmpfolder
        }

        # Save the directory listing and other info in the HarvestObject
        # serialise the dictionary
        harvest_object.content = json.dumps(retobj)
        harvest_object.save()
        return True

    # =======================================================================
    # IMPORT Stage
    # =======================================================================

    def import_stage(self, harvest_object):
        """
        Importing the fetched files into CKAN storage.
        Runs once for each fetched resource.

        :param harvest_object: HarvesterObject instance

        :returns: True if the create or update occurred ok,
                  'unchanged' if it didn't need updating or
                  False if there were errors.
        :rtype: bool|string
        """
        log.info('=====================================================')
        log.info('In %s FTPHarvester import_stage' % self.harvester_name)
        stage = 'Import'

        if not harvest_object or not harvest_object.content:
            log.error('No harvest object received: %s' % harvest_object)
            self._save_object_error('Empty content for harvest object %s' % harvest_object.id, harvest_object, stage)
            return False

        try:
            obj = json.loads(harvest_object.content)
        except JSONDecodeError as e:
            log.error('Invalid harvest object: %s', harvest_object)
            self._save_object_error('Unable to decode harvester info: %s' % str(e), harvest_object, stage)
            return None

        # set harvester config
        self.config = json.loads(harvest_object.job.source.config)

        if obj['type'] == 'finalizer':
            self.finalize(obj['tempdir'])
            return True

        f = obj.get('file')
        if not f:
            log.error('Invalid file key in harvest object: %s' % obj)
            self._save_object_error('No file to import', harvest_object, stage)
            return None

        tmpfolder = obj.get('tmpfolder')
        if not tmpfolder:
            log.error('invalid tmpfolder in harvest object: %s' % obj)
            self._save_object_error('Could not get path of temporary folder: %s' % tmpfolder, harvest_object, stage)
            return None

        context = {'model': model, 'session': Session, 'user': self._get_user_name()}

        now = datetime.now().isoformat()

        # =======================================================================
        # package
        # =======================================================================

        resource_meta = None

        package_dict = {
            'name': self.config['dataset'].lower(),
            'identifier': self.config['dataset']  # required by DCAT extension
        }

        try:

            # -----------------------------------------------------------------------
            # use the existing package dictionary (if it exists)
            # -----------------------------------------------------------------------

            # add package_show to the auth audit stack
            dataset = self._find_existing_package({'id': package_dict.get('name')})

            if not dataset or 'id' not in dataset:
                # abort updating
                log.error("Package '%s' not found" % package_dict.get('name'))
                raise NotFound("Package '%s' not found" % package_dict.get('name'))

            # update version of package
            dataset['version'] = now

            # check if there is a resource matching the filename in the package
            resource_meta = self.find_resource_in_package(dataset, f)
            log.debug('Found existing resource: %s' % str(resource_meta))

            log.info("Using existing package with id %s" % str(dataset.get('id')))

        except NotFound:
            # -----------------------------------------------------------------------
            # create the package dictionary instead
            # -----------------------------------------------------------------------

            # add the metadata from the harvester
            package_dict = self._add_harvester_metadata(package_dict, context)

            # title of the package
            if 'title' not in package_dict:
                package_dict['title'] = {
                    "de": self.config['dataset'],
                    "en": self.config['dataset'],
                    "fr": self.config['dataset'],
                    "it": self.config['dataset']
                }
            # for DCAT schema - same info as in the title
            if 'display_name' not in package_dict:
                package_dict['display_name'] = package_dict['title']

            package_dict['creator_user_id'] = model.User.get(context['user']).id

            # fill with empty defaults
            for key in ['issued', 'modified', 'metadata_created', 'metadata_modified']:
                if key not in package_dict:
                    package_dict[key] = now
            for key in ['resources', 'groups', 'tags', 'extras', 'contact_points', 'relations',
                        'relationships_as_object', 'relationships_as_subject', 'publishers', 'see_alsos', 'temporals']:
                if key not in package_dict:
                    package_dict[key] = []
            for key in ['keywords']:
                if key not in package_dict:
                    package_dict[key] = {}

            package_dict['source_type'] = self.info()['name']

            # count keywords or tags
            package_dict['num_tags'] = 0
            tags = package_dict.get('keywords') if package_dict.get('keywords', {}) else package_dict.get('tags', {})
            # count the english tags (if available)
            if tags and 'en' in tags and isinstance(tags['en'], list):
                package_dict['num_tags'] = len(tags['en'])

            if 'language' not in package_dict:
                package_dict['language'] = ["en", "de", "fr", "it"]

            package_dict = self._add_package_tags(package_dict)
            package_dict = self._add_package_groups(package_dict, context)
            source_org = model.Package.get(harvest_object.source.id).owner_org
            self._save_object_error('Harvester Source %s need an organization set (object %s)' %
                                    (self.harvester_name, harvest_object.id), harvest_object, stage)
            package_dict = self._add_package_orgs(package_dict, context, source_org)
            package_dict = self._add_package_extras(package_dict, harvest_object)

            # version
            package_dict['version'] = now

            # -----------------------------------------------------------------------
            # create the package
            # -----------------------------------------------------------------------

            log.debug("Package dict (pre-creation): %s" % str(package_dict))

            # This logic action requires to call check_access to 
            # prevent the Exception: 'Action function package_show  did not call its auth function'
            # Adds action name onto the __auth_audit stack
            if not check_access('package_create', context):
                self._save_object_error('%s not authorised to create packages (object %s)' %
                                        (self.harvester_name, harvest_object.id), harvest_object, stage)
                return False

            # create the dataset
            dataset = get_action('package_create')(context, package_dict)

            log.info("Created package: %s" % str(dataset['name']))

        except Exception as e:
            # log.error("Error: Package dict: %s" % str(package_dict))
            self._save_object_error('Package update/creation error: %s' % str(e), harvest_object, stage)
            return False

        # need a dataset to continue
        if not dataset:
            self._save_object_error('Could not update or create package: %s' % self.harvester_name, harvest_object,
                                    stage)
            return False

        # TODO
        # ---------------------------------------------
        # associate the harvester with the dataset
        harvest_object.guid = dataset['id']
        harvest_object.package_id = dataset['id']
        # TODO: set the source (?)
        # ---------------------------------------------

        # =======================================================================
        # resource
        # =======================================================================

        log.debug('Importing file: %s' % str(f))

        site_url = ckanconf.get('ckan.site_url', None)
        if not site_url:
            self._save_object_error('Could not get site_url from CKAN config file', harvest_object, stage)
            return False
        site_url = site_url.rstrip('/')

        log.info("Adding %s to package with id %s", str(f), dataset['id'])

        # set mimetypes of resource based on file extension
        na, ext = os.path.splitext(f)
        ext = ext.lstrip('.').upper()

        file_format = self.default_format
        mimetype = self.default_mimetype
        mimetype_inner = self.default_mimetype_inner
        if ext and ext.lower() in helpers.resource_formats():
            # set mime types
            file_format = mimetype = mimetype_inner = ext

        fp = None
        try:
            try:
                size = int(os.path.getsize(f))
            except ValueError:
                size = None

            fp = open(f, 'rb')

            # -----------------------------------------------------
            # create new resource, if it did not previously exist
            # -----------------------------------------------------
            if not resource_meta:
                old_resource_id = None

                if self.resource_dict_meta:
                    resource_meta = self.resource_dict_meta
                else:
                    resource_meta = {}

                resource_meta['identifier'] = os.path.basename(f)

                resource_meta['issued'] = now
                resource_meta['modified'] = now
                resource_meta['version'] = now

                if not resource_meta.get('rights'):
                    resource_meta['rights'] = 'TODO'
                if not resource_meta.get('license'):
                    resource_meta['license'] = 'TODO'
                if not resource_meta.get('coverage'):
                    resource_meta['coverage'] = 'TODO'

                resource_meta['format'] = file_format
                resource_meta['mimetype'] = mimetype
                resource_meta['mimetype_inner'] = mimetype_inner

                for key in ['relations']:
                    resource_meta[key] = []

                resource_meta['name'] = {  # json.dumps(
                        "de": os.path.basename(f),
                        "en": os.path.basename(f),
                        "fr": os.path.basename(f),
                        "it": os.path.basename(f)
                    }
                resource_meta['title'] = {
                        "de": os.path.basename(f),
                        "en": os.path.basename(f),
                        "fr": os.path.basename(f),
                        "it": os.path.basename(f)
                    }
                resource_meta['description'] = {
                        "de": "TODO",
                        "en": "TODO",
                        "fr": "TODO",
                        "it": "TODO"
                    }

                log_msg = "Creating new resource: %s"

            # -----------------------------------------------------
            # create the resource, but use the known metadata (of the old resource)
            # -----------------------------------------------------
            else:
                old_resource_id = resource_meta['id']

                # the resource will get a new revision_id, so we delete that key
                for key in ['id', 'revision_id']:  # TODO: there may be other stuff to delete?
                    if key in resource_meta:
                        del resource_meta[key]

                log_msg = "Updating resource (with known metadata): %s"

            resource_meta['package_id'] = dataset['id']

            # url parameter is ignored for resource uploads, but required by ckan
            # this parameter will be replaced later by the resource patch with a link to the download file
            if 'url' not in resource_meta:
                resource_meta['url'] = 'http://dummy-value'
                resource_meta['download_url'] = 'http://dummy-value'

            if size is not None:
                resource_meta['size'] = size
                resource_meta['byte_size'] = size / 8

            log.info(log_msg % str(resource_meta))

            # step 1: upload the resource's info with requests library to avoid ckanapi json_encode error
            # ---------------------------------------------------------------------
            apikey = model.User.get(context['user']).apikey.encode('utf8')
            # log.debug("Posting to api_url: %s" % str(api_url))
            headers = {
                'Authorization': apikey,
                'X-CKAN-API-Key': apikey,
                'user-agent': 'ftp-harvester/1.0.0',
                'Accept': 'application/json;q=0.9,text/plain;q=0.8,*/*;q=0.7',
                'Content-Type': 'application/json',
            }
            # ------
            api_url = site_url + self._get_action_api_offset() + '/resource_create'
            r = requests.post(api_url, data=json.dumps(resource_meta), headers=headers)
            # ------
            # check result
            if r.status_code != 200:
                r.raise_for_status()
            json_response = r.json()

            if 'success' not in json_response or not json_response['success'] or 'result' not in json_response:
                raise Exception("Resource creation unsuccessful")

            log.info("Successfully created resource")

            # step 2: update the resource with a resolvable url and the correct download_url
            # -----------------------------------------------------------------------

            resource = json_response['result']

            # curl-patch resource
            # -------------------
            log.info('Patching resource')
            filename = munge_filename(os.path.basename(f))
            patch_url = u'%s/dataset/%s/resource/%s/download/%s' % (site_url, dataset['name'], resource['id'], filename)
            api_url = site_url + self._get_action_api_offset() + '/resource_patch'
            try:
                cmd = "curl -H'Authorization: %s' '%s' --form upload=@\"%s\" --form id=%s --form download_url=%s" % \
                      (headers['Authorization'], api_url, f, resource['id'], patch_url)
                subprocess.call(cmd, shell=True)
                log.info("Successfully patched resource")
            except subprocess.CalledProcessError as e:
                self._save_object_error('Error patching resource: %s' % str(e), harvest_object, stage)
                return False
            # -------------------

            # delete the old version of the resource
            if old_resource_id:
                api_url = site_url + self._get_action_api_offset() + '/resource_delete'
                requests.post(api_url, data=json.dumps({'id': old_resource_id}), headers=headers)

            log.info("Successfully harvested file %s" % f)

            # ---------------------------------------------------------------------

        except Exception as e:
            log.error("Error adding resource: %s" % str(e))
            # log.debug(traceback.format_exc())
            self._save_object_error('Error adding resource: %s' % str(e), harvest_object, stage)
            return False

        finally:

            # close the file pointer
            if fp:
                fp.close()

            # remove the downloaded resource
            try:
                os.remove(f)
                log.info("Deleted tmp file %s" % f)
            except OSError:
                pass
        return True

    def finalize(self, tempdir):
        log.info('Running finalizing tasks:')

        log.info('Deleting temp directory')
        self.remove_tmpfolder(tempdir)

        log.info('Ordering resources')
        log.info('Generating permalink')


class ContentFetchError(Exception):
    """ Exception that can be raised when something goes wrong during the fetch stage """
    pass


class RemoteResourceError(Exception):
    """ Exception that can be raised when remote operations fail """
    pass
