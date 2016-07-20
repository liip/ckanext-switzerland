'''
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
'''

# import traceback
import logging
log = logging.getLogger(__name__)

from ckan import model
from ckan.lib.base import c
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound
from ckan.logic import get_action, check_access
from ckan.lib.helpers import json
from ckan.lib.munge import munge_name
from ckan.lib import helpers
from pylons import config as ckanconf

import os
import ftplib # for errors only
import tempfile
import time
from datetime import datetime, date
import shutil
import requests

from base import HarvesterBase
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from simplejson.scanner import JSONDecodeError

from FTPHelper import FTPHelper




class BaseFTPHarvester(HarvesterBase):
    """
    A FTP Harvester for ftp data
    The class can operate on its own.
    However, usually one would create a specific class
    for a harvester and overwrite the base class attributes.
    """

    config = None # ckan harvester config, not ftp config

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
        return os.path.join('/', self.environment, self.remotefolder.lstrip('/')) # e.g. /test/DiDok or /prod/Info+

    # tested
    def _get_local_dirlist(self, localpath="."):
        """
        Get directory listing, including all sub-folders

        :param localpath: Path to a local folder
        :type localpath: str or unicode

        :returns: Directory listing
        :rtype: list
        """
        dirlist = []
        for dirpath, dirnames, filenames in os.walk(localpath):
            for filename in [f for f in filenames]:
                dirlist.append(os.path.join(dirpath, filename))
        return dirlist

    # tested
    def _set_config(self, config_str):
        """
        Set configuration value

        :param localpath: config_str
        :type localpath: str or unicode
        """
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])
            log.debug('Using config: %r', self.config)
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
            'name': '%sharvest' % self.harvester_name.lower(), # 'ckanftp'
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
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'], dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model':model,'user':c.user}
                try:
                    user = get_action('user_show')(context,{'id':config_obj.get('user')})
                except NotFound,e:
                    raise ValueError('User not found')

            for key in ('read_only','force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key], bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError,e:
            raise e

        return config

    # tested
    def _add_harvester_metadata(self, package_dict, context):
        """
        Adds the metadata stored in the harvester class

        :param package_dict: Package metadata
        :type package_dict: dict

        :param context: Context
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
            for key,val in self.package_dict_meta.iteritems():
                package_dict[key] = val

        return package_dict

    # tested
    def _add_package_tags(self, package_dict, context):
        """
        Create tags

        :param package_dict: Package metadata
        :type package_dict: dict

        :returns: Package dictionary
        :rtype: dict
        """
        if not 'tags' in package_dict:
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

    def _add_package_groups(self, package_dict, context):
        """
        Create (default) groups

        :param package_dict: Package metadata
        :type package_dict: dict

        :returns: Package dictionary
        :rtype: dict
        """
        if not 'groups' in package_dict:
            package_dict['groups'] = []

        # Set default groups if needed
        default_groups = self.config.get('default_groups', [])
        # package_dict['groups'].extend([g for g in default_groups if g not in package_dict['groups']])
        # check if groups exist locally, otherwise do not add them
        for group_name in default_groups:
            try:
                group = get_action('group_show')(context, {'id': group_name})
                if self.api_version == 1:
                    package_dict['groups'].append(group['name'])
                else:
                    package_dict['groups'].append(group['id'])
            except NotFound, e:
                log.info('Group %s is not available' % group_name)

        return package_dict

    def _add_package_orgs(self, package_dict, context):
        """
        Create default organization(s)
        
        :param package_dict: Package metadata
        :type package_dict: dict

        :returns: Package dictionary
        :rtype: dict
        """

        # add the organization from the config object
        default_org = self.config.get('organization', False)
        if not default_org:
            return package_dict

        # check if this organization exists
        org_dict = get_action('organization_show')(context, {'id': org})
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
        :type harvest_object: object

        :returns: Package dictionary
        :rtype: dict
        """

        # Set default extras if needed
        default_extras = self.config.get('default_extras', {})
        if default_extras:
            override_extras = self.config.get('override_extras', False)
            if not 'extras' in package_dict:
                package_dict['extras'] = {}
            for key,value in default_extras.iteritems():
                if not key in package_dict['extras'] or override_extras:
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(harvest_source_id=harvest_object.job.source.id,
                                 harvest_source_url=harvest_object.job.source.url.strip('/'),
                                 harvest_source_title=harvest_object.job.source.title,
                                 harvest_job_id=harvest_object.job.id,
                                 harvest_object_id=harvest_object.id,
                                 dataset_id=package_dict['id'])

                    package_dict['extras'][key] = value

        return package_dict

    # tested
    def remove_tmpfolder(self, tmpfolder):
        if not tmpfolder:
            return
        shutil.rmtree(tmpfolder)

    # tested
    def cleanup_after_error(self, retobj):
        if retobj and 'tmpfolder' in retobj:
            self.remove_tmpfolder(retobj['tmpfolder'])

    def find_resource_in_package(self, dataset, filepath, harvest_object):
        resource_meta = None
        if 'resources' in dataset and len(dataset['resources']):
            # Find resource in the existing packages resource list
            for res in dataset['resources']:
                # match the resource by its filename
                if os.path.basename(res.get('url')) != munge_name(os.path.basename(filepath)):
                    continue
                # ignore deleted resources
                if res.get('status') != 'active':
                    continue
                try:
                    # store the resource's metadata for later use
                    resource_meta = res
                    # there should only be one file with the same name in each dataset
                    break
                except Exception as e:
                    log.error("Error deleting the existing resource %s: %s" % (str(res.get('id'), str(e))))
                    pass
        return resource_meta


    # =======================================================================
    # GATHER Stage
    # =======================================================================

    def gather_stage(self, harvest_job):
        """
        Dummy stage that launches the next phase

        :param resource_meta: Harvester job
        :type resource_meta: object

        :returns: List of HarvestObject ids that are processed in the next stage (fetch_stage)
        :rtype: list
        """
        log.debug('=====================================================')
        log.debug('In %s FTPHarvester gather_stage' % self.harvester_name) # harvest_job.source.url
        stage = 'Gather'

        dirlist = []

        # get a listing of all files in the target directory

        remotefolder = self.get_remote_folder()
        log.debug("Getting listing from remotefolder: %s" % remotefolder)

        # modified_dates = {}

        try:

            with FTPHelper(remotefolder) as ftph:

                dirlist = ftph.get_remote_dirlist()
                log.debug("Remote dirlist: %s" % str(dirlist))

                # get last-modified date of each file
                # for file in dirlist:
                #     modified_dates[file] = ftph.get_modified_date(file)

                # store some config for the next step

                # ftplib stores retrieved files in a folder, e.g. 'ftp-secure.sbb.ch:990'
                ftplibfolder = ftph.get_top_folder()
                # log.debug('Topfolder: %s' % ftplibfolder)

                # set base directory of the tmp folder
                tmpdirbase = os.path.join(ftph._config['localpath'], ftplibfolder.strip('/'), remotefolder.lstrip('/'))
                tempfile.tempdir = tmpdirbase

                # the base tmp folder needs to be created for the tempfile library
                if not os.path.exists(tmpdirbase):
                    ftph.create_local_dir(tmpdirbase)

                # set prefix for tmp folder
                prefix = datetime.now().strftime(self.tmpfolder_prefix)
                # save the folder path where the files are to be downloaded
                # all parts following the first one must be relative paths

                workingdir = tempfile.mkdtemp(prefix=prefix)
                log.debug("Created workingdir: %s" % workingdir)


        except ftplib.all_errors as e:
            self._save_gather_error('Error getting remote directory listing: %s' % str(e), harvest_job)
            return None

        log.debug("Remote dirlist: %s" % str(dirlist))

        if not len(dirlist):
            self._save_gather_error('No files found in %s' % remotefolder, harvest_job)
            return None


        # version 1: create one harvest object for the package
        # -------------------------------------------------------------------------
        # harvest_object = HarvestObject(guid=self.harvester_name, job=harvest_job)
        # # serialise and store the dirlist
        # harvest_object.content = json.dumps(dirlist)
        # # save it for the next step
        # harvest_object.save()
        # return [ harvest_object.id ]

        # version 2: create one harvest job for each resource in the package
        # -------------------------------------------------------------------------
        object_ids = []

        # TODO
        # ------------------------------------------------------
        # 1: only download the resources that have been modified
        # has there been a previous run and was it successful?
        # previous_job = Session.query(HarvestJob) \
        #                 .filter(HarvestJob.source==harvest_job.source) \
        #                 .filter(HarvestJob.gather_finished!=None) \
        #                 .filter(HarvestJob.id!=harvest_job.id) \
        #                 .order_by(HarvestJob.gather_finished.desc()) \
        #                 .limit(1).first()
        # if previous_job and not previous_job.gather_errors and previous_job.objects and len(previous_job.objects):
        #     # optional force_all config setting can be used to always download all files
        #     if not self.config or not self.config.get('force_all', False):
        #         # Request only the resources modified since last harvest job
        #         last_time = previous_job.gather_finished.isoformat()
        #         for file in dirlist:
        #             # TODO: compare the modified date of the file with the harvester run
        # run MDMT command for each file
        #             pass
        #             # else:
        #             #     log.info('No packages have been updated on the remote CKAN instance since the last harvest job')
        #             #     return []

        # ------------------------------------------------------
        # 2: download all resources
        # else:

        for file in dirlist:

            obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
            # serialise and store the dirlist
            obj.content = json.dumps({
                'file': file,
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
        Fetching of resources

        :param harvest_object: HarvesterObject instance
        :type harvest_object: object

        :returns: Whether HarvestObject was saved or not
        :rtype: mixed
        """
        log.debug('=====================================================')
        log.debug('In %s FTPHarvester fetch_stage' % self.harvester_name)
        log.debug('Running harvest job %s' % harvest_object.id)
        stage = 'Fetch'

        # self._set_config(harvest_job.source.config)

        if not harvest_object or not harvest_object.content:
            log.error('No harvest object received')
            self._save_object_error('No harvest object received', harvest_object, stage)
            return None

        try:
            obj = json.loads(harvest_object.content)
        except JSONDecodeError as e:
            log.error('Invalid harvest object: %s' % obj)
            self._save_gather_error('Unable to decode harvester info: %s' % str(e), harvest_job)
            return None

        # the file to harvest from the previous step
        file = obj.get('file')
        if not file:
            self._save_object_error('No file to harvest: %s' % harvest_object.content, harvest_object, stage)
            return None

        # the folder where the file is to be stored
        tmpfolder = obj.get('workingdir')
        if not tmpfolder:
            self._save_object_error('No tmpfolder received from gather step: %s' % harvest_object.content, harvest_object, stage)
            return None

        # the folder where the file is to be stored
        remotefolder = obj.get('remotefolder')
        if not remotefolder:
            self._save_object_error('No remotefolder received from gather step: %s' % harvest_object.content, harvest_object, stage)
            return None


        log.debug("Remote directory: %s" % remotefolder)
        log.debug("Local directory: %s"  % tmpfolder)


        try:

            with FTPHelper(remotefolder) as ftph:

                # fetch file via ftplib
                # -------------------------------------------------------------------
                # full path of the destination file
                targetfile = os.path.join(tmpfolder, file)

                log.debug('Fetching file: %s' % str(file))

                start = time.time()
                status = ftph.fetch(file, targetfile) # 226 Transfer complete
                elapsed = time.time() - start

                log.debug("Fetched %s [%s] in %ds" % (file, str(status), elapsed))

                if status != '226 Transfer complete':
                    self._save_object_error('Download error for file %s: %s' % (file, str(status)), harvest_object, stage)
                    return None

                if self.do_unzip:
                    ftph.unzip(targetfile)

        except ftplib.all_errors as e:
            self._save_object_error('Ftplib error: %s' % str(e), harvest_object, stage)
            self.cleanup_after_error(tmpfolder)
            return None

        except Exception as e:
            self._save_object_error('An error occurred: %s' % e, harvest_object, 'Fetch')
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
        Importing the fetched files into CKAN storage

        :param harvest_object: HarvesterObject instance
        :type harvest_object: object

        :returns: True if the create or update occurred ok,
                  'unchanged' if it didn't need updating or
                  False if there were errors.
        :rtype: bool|string
        """
        log.debug('=====================================================')
        log.debug('In %s FTPHarvester import_stage' % self.harvester_name) # harvest_job.source.url
        stage = 'Import'

        if not harvest_object or not harvest_object.content:
            log.error('No harvest object received: %s' % harvest_object)
            self._save_object_error('Empty content for harvest object %s' % harvest_object.id, harvest_object, stage)
            return False

        try:
            obj = json.loads(harvest_object.content)
        except JSONDecodeError as e:
            log.error('Invalid harvest object: %s' % obj)
            self._save_gather_error('Unable to decode harvester info: %s' % str(e), harvest_job)
            return None

        file = obj.get('file')
        if not file:
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

        dataset = None
        resource_meta = None

        package_dict = {
            'name': self.harvester_name.lower(), # self.remotefolder # self._ensure_name_is_unique(os.path.basename(self.remotefolder))
            'identifier': self.harvester_name.title() # required by DCAT extension
        }

        try:

            # -----------------------------------------------------------------------
            # use the existing package dictionary (if it exists)
            # -----------------------------------------------------------------------

            # add package_show to the auth audit stack
            # result = check_access('package_show', context)
            # dataset = get_action('package_show')(context, {'id': package_dict.get('name')})
            dataset = self._find_existing_package({'id': package_dict.get('name')})

            if not dataset or not 'id' in dataset:
                # abort updating
                log.debug("Package '%s' not found" % package_dict.get('name'))
                raise NotFound("Package '%s' not found" % package_dict.get('name'))

            # TODO: update version of package
            dataset['version'] = now

            # check if there is a resource matching the filename in the package
            resource_meta = self.find_resource_in_package(dataset, file, harvest_object)

            log.debug("Using existing package with id %s" % str(dataset.get('id')))

        except NotFound as e:
            # -----------------------------------------------------------------------
            # create the package dictionary instead
            # -----------------------------------------------------------------------

            # add the metadata from the harvester
            package_dict = self._add_harvester_metadata(package_dict, context)

            # title of the package
            if not 'title' in package_dict:
                package_dict['title'] = {
                    "de": self.remotefolder.title(),
                    "en": self.remotefolder.title(),
                    "fr": self.remotefolder.title(),
                    "it": self.remotefolder.title()
                }
            # for DCAT schema - same info as in the title
            if not 'display_name' in package_dict:
                package_dict['display_name'] = package_dict['title']

            package_dict['creator_user_id'] = model.User.get(context['user']).id

            # fill with empty defaults
            for key in ['issued', 'modified', 'metadata_created', 'metadata_modified']:
                if not key in package_dict:
                    package_dict[key] = now
            for key in ['resources', 'groups', 'tags', 'extras', 'contact_points', 'relations', 'relationships_as_object', 'relationships_as_subject', 'publishers', 'see_alsos', 'temporals']:
                if not key in package_dict:
                    package_dict[key] = []
            for key in ['keywords']:
                if not key in package_dict:
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

            if not 'language' in package_dict:
                package_dict['language'] = ["en", "de", "fr", "it"]

            # In the harvester interface, certain options can be provided in the config field as a json object
            # The following functions check and add these optional fields
            # TODO: make the functions compatible with multi-lang
            if not self.config:
                self.config = {}
            package_dict = self._add_package_tags(package_dict, context)
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
                self._save_object_error('%s not authorised to create packages (object %s)' % (self.harvester_name, harvest_object.id), harvest_object, stage)
                return False

            # create the dataset
            dataset = get_action('package_create')(context, package_dict)

            log.info("Created package: %s" % str(dataset['name']))

        except Exception as e:
            raise # debug
            # log.error("Error: Package dict: %s" % str(package_dict))
            self._save_object_error('Package update/creation error: %s' % str(e), harvest_object, stage)
            return False


        # need a dataset to continue
        if not dataset:
            self._save_object_error('Could not update or create package: %s' % self.harvester_name, harvest_object, stage)
            return False


        # TODO
        # associate the harvester with the dataset
        harvest_object.guid = dataset['id']
        harvest_object.package_id = dataset['id']



        # =======================================================================
        # resource
        # =======================================================================

        log.debug('Importing file: %s' % str(file))

        site_url = ckanconf.get('ckan.site_url', None)
        if not site_url:
            self._save_object_error('Could not get site_url from CKAN config file', harvest_object, stage)
            return False
        site_url = site_url.rstrip('/')


        log.debug("Adding %s to package with id %s" % (str(file), dataset['id']))


        # set mimetypes of resource based on file extension
        na, ext = os.path.splitext(file)
        ext = ext.lstrip('.').upper()
        # fallback to TXT mimetype for files that do not have an extension
        if not ext:
            file_format = self.default_format
            mimetype = self.default_mimetype
            mimetype_inner = self.default_mimetype_inner
        # if file has an extension
        else:
            # mimetype validation
            # see https://github.com/ckan/ckan/blob/a28b95c0f027c59004cf450fe521f710e5b4470b/ckan/config/resource_formats.json
            if ext.lower() in helpers.resource_formats():
                # set mime types
                file_format = mimetype = mimetype_inner = ext
            # if format is unknown, we don't set it

            # TODO: find out what the inner mimetype is inside of archives
            # if ext in ['ZIP', 'RAR', 'TAR', 'TAR.GZ', '7Z']:
                # mimetype_inner = 'TXT'
                # pass

        try:

            try:
                size = int(os.path.getsize(file))
            except:
                size = None

            fp = open(file, 'rb')

            # -----------------------------------------------------
            # create new resource, if it did not previously exist
            # -----------------------------------------------------
            if not resource_meta:

                # globally defined default values for the resource from the harvester
                if self.resource_dict_meta:
                    resource_meta = self.resource_dict_meta
                else:
                    resource_meta = {}

                api_url = site_url + self._get_action_api_offset() + '/resource_create'

                resource_meta = {}

                resource_meta['identifier'] = os.path.basename(file)

                resource_meta['issued'] = now
                resource_meta['modified'] = now

                resource_meta['version'] = now

                # TODO - it does not really make sense having to set this here
                resource_meta['language'] = ['en','de','fr','it']

                # TODO
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

                resource_meta['name'] = { # json.dumps(
                        "de": os.path.basename(file),
                        "en": os.path.basename(file),
                        "fr": os.path.basename(file),
                        "it": os.path.basename(file)
                    }
                resource_meta['title'] = {
                        "de": os.path.basename(file),
                        "en": os.path.basename(file),
                        "fr": os.path.basename(file),
                        "it": os.path.basename(file)
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

                # the resource should get a new id, so delete the old one
                # the resource will get a new revision_id, so we delete that key
                for key in ['id', 'revision_id']:
                    try:
                        del resource_meta[key]
                    except KeyError as e:
                        pass

                log_msg = "Creating resource (with known metadata): %s"


            resource_meta['package_id'] = dataset['id']

            # TODO
            # url parameter is ignored for resource uploads, but required by ckan
            if not 'url' in resource_meta:
                resource_meta['url'] = 'http://dummy-value'
                # why is this required here? It should be filled out by the extension
                resource_meta['download_url'] = 'http://dummy-value'


            # resource_meta['resource_type'] = 'file'


            # web interface complained about tracking_summary.total not being available in view
            # TODO: why is this not being created by default?
            # tracking_summary = model.TrackingSummary.get_for_resource(resource_meta['url'])
            # if tracking_summary:
            #     resource_meta['tracking_summary'] = tracking_summary
            # else:
            #     resource_meta['tracking_summary'] = {'total' : 0, 'recent' : 0}

            if size != None:
                resource_meta['size'] = size
                resource_meta['byte_size'] = size / 8

            log.info(log_msg % str(resource_meta))

            # upload with requests library to avoid ckanapi json_encode error
            # ---------------------------------------------------------------------
            apikey = model.User.get(context['user']).apikey.encode('utf8')
            log.debug("Posting to api_url: %s" % str(api_url))
            headers = {
                'Authorization': apikey,
                'X-CKAN-API-Key': apikey,
                'user-agent': 'ftp-harvester/1.0.0',
                'Accept': 'application/json;q=0.9,text/plain;q=0.8,*/*;q=0.7',
                # 'Content-Type': 'multipart/form-data', # http://stackoverflow.com/a/18590706/426266
                'Content-Type': 'application/json',
            }
            # ------
            r = requests.post(api_url, data=json.dumps(resource_meta), headers=headers)
            # log.debug('Response: %s' % r.text)
            # ------
            # check result
            if r.status_code != 200:
                r.raise_for_status()
            json_response = r.json()
            if not 'success' in json_response or not json_response['success'] or not 'result' in json_response:

                raise Exception("Upload not successful")

            else:

                log.debug("Successfully created resource")

                # bug fix for the url: patch resource with a url value that will resolve
                # log.debug('json_response: %s' % str(json_response))

                # update the resource with a resolvable url and the correct download_url
                # -----------------------------------------------------------------------

                resource = json_response['result']

                filename, file_extension = os.path.splitext(os.path.basename(file))
                # CKAN insanity
                file_name = munge_name(filename)
                file_extension = file_extension.lower()

                # update the resource
                # -------------------
                # patch_url = u'%s/dataset/%s/resource/%s/download/%s%s' % (site_url, dataset['name'], resource['id'], file_name, file_extension)
                # resource[u'resource_type'] = u'file'
                # resource[u'download_url'] = patch_url
                # resource['title'] = json.dumps(resource[u'title'])
                # resource['description'] = json.dumps(resource[u'description'])
                # if 'url' in resource:
                #     del resource['url'] # in hopes that it will auto-generate a url
                # if 'file' in resource:
                #     del resource['file']
                # log.debug('Updating resource')
                # log.debug('resource dict: %s' % resource)
                # api_url = site_url + self._get_action_api_offset() + '/resource_update'
                # # log.debug('api_url: %s' % api_url)
                # del headers['Content-Type']
                # r = requests.post(api_url, data=resource, files={'file': fp}, headers=headers)
                # log.info("Successfully updated resource")
                # -------------------

                # patch the resource
                # -------------------
                # patch_url = u'%s/dataset/%s/resource/%s/download/%s%s' % (site_url, dataset['name'], resource['id'], file_name, file_extension)
                # patch_dict = {
                #     'id': resource['id'],
                #     # 'download_url': patch_url,
                #     # u'url': patch_url,
                #     # 'resource_type': u'file',
                #     # 'file': fp,
                # }
                # log.debug('Patching resource')
                # if 'Content-Type' in headers:
                #     del headers['Content-Type']
                # log.debug(headers)
                # # headers['Content-Type'] = 'multipart/form-data'
                # api_url = site_url + self._get_action_api_offset() + '/resource_patch'
                # log.debug('api_url: %s' % api_url)
                # r = requests.post(api_url, data=patch_dict, files={'file': fp}, headers=headers)
                # log.info("Successfully patched resource")
                # -------------------

                # curl-patch resource
                # -------------------
                import subprocess
                api_url = site_url + self._get_action_api_offset() + '/resource_patch'
                try:
                    cmd = "curl -H'Authorization: %s' '%s' --form upload=@\"%s\" --form id=%s" % (headers['Authorization'], api_url, file, resource['id'])
                    log.debug('Running cmd: %s' % cmd)
                    subprocess.call(cmd, shell=True)
                except CalledProcessError as e:
                    self._save_object_error('Error patching resource: %s' % str(e), harvest_object, stage)
                    return False
                # -------------------

                if r.status_code != 200:
                    r.raise_for_status()

                json_response = r.json()
                log.debug(json_response)


                log.info("Successfully harvested file %s" % file)


            # ---------------------------------------------------------------------

        except Exception as e:
            log.error("Error adding resource: %s" % str(e))
            # log.debug(traceback.format_exc())
            self._save_object_error('Error adding resource: %s' % str(e), harvest_object, stage)
            return False


        finally:

            # close the file pointer
            try:
                fp.close()
            except:
                pass

            # remove the downloaded resource
            try:
                os.remove(file)
                log.info("Deleted tmp file %s" % file)
            except:
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
    pass

class RemoteResourceError(Exception):
    pass

class CmdError(Exception):
    pass
