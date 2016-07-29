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
from ckan.lib.base import c
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

from FTPHelper import FTPHelper


log = logging.getLogger(__name__)


class BaseFTPHarvester(HarvesterBase):
    """
    A FTP Harvester for ftp data
    The class can operate on its own.
    However, usually one would create a specific class
    for a harvester and overwrite the base class attributes.
    """

    config = None  # ckan harvester config, not ftp config

    # package metadata - each harvester should overwrite this with meta data fields
    package_dict_meta = {}

    api_version = 2
    action_api_version = 3

    # default harvester id, to be overwritten by child classes
    harvester_name = 'ckanftp'

    # default remote directory to harvest, to be overwritten by child classes
    # e.g. infodoc or didok
    remotefolder = ''

    # parent folder of the above remote folder
    environment = 'test'

    # if a resource is uploaded with a format, it will show a tag on the dataset, e.g. XML or TXT
    # the default setting is defined to be TXT for files with no extension
    default_format = 'TXT'
    default_mimetype = 'TXT'
    default_mimetype_inner = 'TXT'

    tmpfolder_prefix = "%d%m%Y-%H%M-"

    do_unzip = True

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
        environment = None
        if self.config:
            environment = self.config.get('environment')
        if not environment:
            environment = self.environment
        return os.path.join('/', environment, self.remotefolder.lstrip('/'))  # e.g. /test/DiDok or /prod/Info+

    # tested
    def _set_config(self, config_str):
        """
        Set configuration value

        :param config_str: Configuration as serialised JSON object
        :type config_str: str or unicode
        """
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])
        else:
            self.config = {}

    # tested
    def info(self):
        """
        Return basic information about the harvester

        :returns: Dictionary with basic information about the harvester
        :rtype: dict
        """
        return {
            'name': '%sharvest' % self.harvester_name.lower(),  # 'ckanftp'
            'title': 'CKAN FTP %s Harvester' % self.harvester_name,
            'description': 'Fetches %s' % self.get_remote_folder(),
            'form_config_interface': 'Text'
        }

    def validate_config(self, config):
        """
        Validates the configuration that can be pasted into the harvester web interface

        :param config: Configuration (JSON-encoded object)
        :type config: dict

        :returns: Configuration dictionary
        :rtype: dict
        """

        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'], list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'], list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model': model, 'user': c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        get_action('group_show')(context, {'id': group_name})
                    except NotFound:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'], dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model': model, 'user': c.user}
                try:
                    get_action('user_show')(context, {'id': config_obj.get('user')})
                except NotFound:
                    raise ValueError('User not found')

            for key in ('read_only', 'force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key], bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError as e:
            raise e

        return config

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
            # get organization dictionary based on the owner_org id
            if self.package_dict_meta.get('owner_org'):
                # get the organisation and add it to the package
                result = check_access('organization_show', context)
                if result:
                    org_dict = get_action('organization_show')(context, {'id': self.package_dict_meta['owner_org']})
                    if org_dict:
                        package_dict['organization'] = org_dict
                    else:
                        package_dict['owner_org'] = None
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

        # add optional tags, defined in the harvester
        # if self.package_dict_meta.get('tags'):
        #     for tag in self.package_dict_meta.get('tags'):
        #         tag_dict = get_action('tag_show')(context, {'id': tag})
        #         if tag_dict:
        #             # add the found tag to the package's tags
        #             package_dict['tags'].append(tag_dict)

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

    def _add_package_orgs(self, package_dict, context):
        """
        Create default organization(s)
        
        :param package_dict: Package metadata
        :type package_dict: dict
        :param context: CKAN context
        :type context: dict

        :returns: Package dictionary
        :rtype: dict
        """

        # add the organization from the config object
        default_org = self.config.get('organization', False) or ckanconf.get('ckan.ftp.organization', False)

        if not default_org:
            return package_dict

        # check if this organization exists
        org_dict = get_action('organization_show')(context, {'id': default_org})
        if org_dict:
            package_dict['owner_org'] = default_org
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
                # TODO: ignore deleted resources
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
        self._set_config(harvest_job.source.config)

        modified_dates = {}

        # get a listing of all files in the target directory

        remotefolder = self.get_remote_folder()
        log.info("Getting listing from remotefolder: %s" % remotefolder)

        try:
            with FTPHelper(remotefolder) as ftph:
                dirlist = ftph.get_remote_dirlist()
                log.debug("Remote dirlist: %s" % str(dirlist))

                # get last-modified date of each file
                for f in dirlist:
                    modified_dates[f] = ftph.get_modified_date(f)

                # store some config for the next step

                # ftplib stores retrieved files in a folder, e.g. 'ftp-secure.sbb.ch:990'
                ftplibfolder = ftph.get_top_folder()

                # set base directory of the tmp folder
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

        if not len(dirlist):
            self._save_gather_error('No files found in %s' % remotefolder, harvest_job)
            return None

        # create one harvest job for each resource in the package
        # -------------------------------------------------------------------------
        object_ids = []

        # TODO
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
                for f in dirlist[:]:
                    modified_date = modified_dates.get(f)
                    if modified_date and modified_date < previous_job.gather_started:
                        # do not run the harvest for this file
                        dirlist.remove(f)

                if not len(dirlist):
                    log.info('No files have been updated on the ftp server since the last harvest job')
                    return []  # no files to harvest this time
        # ------------------------------------------------------

        # ------------------------------------------------------
        # 2: download all resources
        for f in dirlist:
            obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
            # serialise and store the dirlist
            obj.content = json.dumps({
                'file': f,
                'workingdir': workingdir,
                'remotefolder': remotefolder
            })
            # save it for the next step
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

        # self._set_config(harvest_job.source.config)

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

                # TODO: UNZIP FILE AND SPAWN HARVESTER JOBS -------------------------
                # if self.do_unzip:
                #     # unzip the file
                #     file_num = ftph.unzip(targetfile)
                #     if file_num:
                #         # process the new files
                #         unzipped_dirlist = ftph.get_local_dirlist(tmpfolder)
                #         for f in unzipped_dirlist:
                #             # do not create a job for the zip file itself -> skip it
                #             if os.path.basename(targetfile) == os.path.basename(f):
                #                 continue
                #             # TODO
                #             job = HarvestJob()
                #             job.source = self.harvester_name.lower()
                #             job.save()
                #             new_obj = HarvestObject(guid=self.harvester_name, job=job)
                #             # serialise and store the dirlist
                #             new_obj.content = json.dumps({
                #                 'file': f,
                #                 'tmpfolder': tmpfolder,
                #             })
                #             # save it
                #             new_obj.save()
                #             # log.info('Harvest object saved %s', job.id)
                #             # run the harvest job
                #             # get_action('harvest_send_job_to_gather_queue')(context, {'id': job.id})
                #             ret = self.import_stage(new_obj)
                #             if not ret:
                #                 self._save_object_error('An error occurred when extracting the file %s from zip' % \
                #                      os.path.basename(f), new_obj, stage)
                #                 # TODO: how should the error be handled?
                # -------------------------------------------------------------------

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

        # set harvester config
        self._set_config(harvest_object.job.source.config)

        now = datetime.now().isoformat()

        # =======================================================================
        # package
        # =======================================================================

        resource_meta = None

        package_dict = {
            'name': self.harvester_name.lower(),
            # TODO: identifier should be the package's id (which is unknown at this point in time)
            'identifier': self.harvester_name.title()  # required by DCAT extension
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
                    "de": self.remotefolder.title(),
                    "en": self.remotefolder.title(),
                    "fr": self.remotefolder.title(),
                    "it": self.remotefolder.title()
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

            # TODO
            # package_dict['type'] = self.info()['name']
            # package_dict['type'] = u'harvest'

            # TODO
            package_dict['source_type'] = self.info()['name']

            # count keywords or tags
            package_dict['num_tags'] = 0
            tags = package_dict.get('keywords') if package_dict.get('keywords', {}) else package_dict.get('tags', {})
            # count the english tags (if available)
            if tags and 'en' in tags and isinstance(tags['en'], list):
                package_dict['num_tags'] = len(tags['en'])

            if 'language' not in package_dict:
                package_dict['language'] = ["en", "de", "fr", "it"]

            # In the harvester interface, certain options can be provided in the config field as a json object
            # The following functions check and add these optional fields
            # TODO: make the functions compatible with multi-lang
            if not self.config:
                self.config = {}
            package_dict = self._add_package_tags(package_dict)
            package_dict = self._add_package_groups(package_dict, context)
            package_dict = self._add_package_orgs(package_dict, context)
            package_dict = self._add_package_extras(package_dict, harvest_object)

            # version
            package_dict['version'] = now

            # -----------------------------------------------------------------------
            # create the package
            # -----------------------------------------------------------------------

            log.debug("Package dict (pre-creation): %s" % str(package_dict))

            # This logic action requires to call check_access to 
            # prevent the Exception: 'Action function package_show did not call its auth function'
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
                if self.resource_dict_meta:
                    resource_meta = self.resource_dict_meta
                else:
                    resource_meta = {}

                api_url = site_url + self._get_action_api_offset() + '/resource_create'

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

                api_url = site_url + self._get_action_api_offset() + '/resource_update'

                # a resource was found - use the existing metadata from this resource

                # the resource will get a new revision_id, so we delete that key
                for key in ['revision_id']:  # TODO: there may be other stuff to delete?
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

        # ---------------------------------------------------------------------
        # TODO:
        # the harvest job of the last resource needs to clean and remove the tmpfolder
        # ---------------------------------------------------------------------
        # do this somewhere else:
        # if harvest_object.get('import_finished') != None:
        #     self.remove_tmpfolder(harvest_object.content.get('tmpfolder'))
        # ---------------------------------------------------------------------
        # =======================================================================
        return True


class ContentFetchError(Exception):
    """ Exception that can be raised when something goes wrong during the fetch stage """
    pass


class RemoteResourceError(Exception):
    """ Exception that can be raised when remote operations fail """
    pass
