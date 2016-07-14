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
from simplejson.scanner import JSONDecodeError
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

    tmpfile_extension = '.TMP'

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


    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version
    def _get_action_api_offset(self):
        return '/api/%d/action' % self.action_api_version
    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version


    def get_remote_folder(self):
        return os.path.join('/', self.environment, self.remotefolder.lstrip('/')) # e.g. /test/DiDok or /prod/Info+

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

    # TODO
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

    def remove_tmpfolder(self, tmpfolder):
        if not tmpfolder:
            return
        shutil.rmtree(tmpfolder)

    def cleanup_after_error(self, retobj):
        if retobj and retobj.get('tmpfolder'):
            self.remove_tmpfolder(retobj['tmpfolder'])


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

        try:

            with FTPHelper(remotefolder) as ftph:

                dirlist = ftph.get_remote_dirlist()

                # .TMP must be ignored, as they are still being uploaded
                dirlist = filter(lambda x: not x.lower().endswith(self.tmpfile_extension), dirlist)

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


        # send the jobs to the gather queue
        return object_ids



        # -----
        # TODO: implement a function with the below code that allows to resume downloads / only load changed resources

        # get_all_packages = True
        # package_ids = []

        # # Check if this source has been harvested before
        # previous_job = Session.query(HarvestJob) \
        #                 .filter(HarvestJob.source==harvest_job.source) \
        #                 .filter(HarvestJob.gather_finished!=None) \
        #                 .filter(HarvestJob.id!=harvest_job.id) \
        #                 .order_by(HarvestJob.gather_finished.desc()) \
        #                 .limit(1).first()

        # # Get source URL
        # base_url = harvest_job.source.url.rstrip('/')
        # base_rest_url = base_url + self._get_rest_api_offset()
        # base_search_url = base_url + self._get_search_api_offset()

        # # Filter in/out datasets from particular organizations
        # org_filter_include = self.config.get('organizations_filter_include', [])
        # org_filter_exclude = self.config.get('organizations_filter_exclude', [])
        # def get_pkg_ids_for_organizations(orgs):
        #     pkg_ids = set()
        #     for organization in orgs:
        #         url = base_search_url + '/dataset?organization=%s' % organization
        #         content = self._get_content(url)
        #         content_json = json.loads(content)
        #         result_count = int(content_json['count'])
        #         pkg_ids |= set(content_json['results'])
        #         while len(pkg_ids) < result_count or not content_json['results']:
        #             url = base_search_url + '/dataset?organization=%s&offset=%s' % (organization, len(pkg_ids))
        #             content = self._get_content(url)
        #             content_json = json.loads(content)
        #             pkg_ids |= set(content_json['results'])
        #     return pkg_ids
        # include_pkg_ids = get_pkg_ids_for_organizations(org_filter_include)
        # exclude_pkg_ids = get_pkg_ids_for_organizations(org_filter_exclude)

        # if (previous_job and not previous_job.gather_errors and not len(previous_job.objects) == 0):
        #     if not self.config.get('force_all',False):
        #         get_all_packages = False

        #         # Request only the packages modified since last harvest job
        #         last_time = previous_job.gather_finished.isoformat()
        #         url = base_search_url + '/revision?since_time=%s' % last_time

        #         try:
        #             content = self._get_content(url)

        #             revision_ids = json.loads(content)
        #             if len(revision_ids):
        #                 for revision_id in revision_ids:
        #                     url = base_rest_url + '/revision/%s' % revision_id
        #                     try:
        #                         content = self._get_content(url)
        #                     except ContentFetchError,e:
        #                         self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
        #                         continue

        #                     revision = json.loads(content)
        #                     package_ids = revision['packages']
        #             else:
        #                 log.info('No packages have been updated on the remote CKAN instance since the last harvest job')
        #                 return []

        #         except urllib2.HTTPError,e:
        #             if e.getcode() == 400:
        #                 log.info('CKAN instance %s does not suport revision filtering' % base_url)
        #                 get_all_packages = True
        #             else:
        #                 self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
        #                 return None

        # if get_all_packages:
        #     # Request all remote packages
        #     url = base_rest_url + '/package'

        #     try:
        #         content = self._get_content(url)
        #         package_ids = json.loads(content)
        #     except ContentFetchError,e:
        #         self._save_gather_error('Unable to get content for URL: %s: %s' % (url, str(e)),harvest_job)
        #         return None
        #     except JSONDecodeError,e:
        #         self._save_gather_error('Unable to decode content for URL: %s: %s' % (url, str(e)),harvest_job)
        #         return None

        # if org_filter_include:
        #     package_ids = set(package_ids) & include_pkg_ids
        # elif org_filter_exclude:
        #     package_ids = set(package_ids) - exclude_pkg_ids

        # try:
        #     object_ids = []
        #     if len(package_ids):
        #         for package_id in package_ids:
        #             # Create a new HarvestObject for this identifier
        #             obj = HarvestObject(guid = package_id, job = harvest_job)
        #             obj.save()
        #             object_ids.append(obj.id)

        #         return object_ids

        #     else:
        #        self._save_gather_error('No packages received for URL: %s' % url,
        #                harvest_job)
        #        return None
        # except Exception, e:
        #     self._save_gather_error('%r'%e.message,harvest_job)


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

        # except CmdError as e:
        #     self._save_object_error('Cmd error: %s' % str(e), harvest_object, stage)
        #     return None
        # except subprocess.CalledProcessError as e:
        #     self._save_object_error('WGet Error [%d]: %s' % (e.returncode, e), harvest_object, stage)
        #     return None

        except Exception as e:
            self._save_object_error('An error occurred: %s' % e, harvest_object, 'Fetch')
            self.cleanup_after_error(tmpfolder)
            return None


        # get an updated list of all local files (extracted and zip)
        # dirlist = self._get_local_dirlist(workingdir)

        # log.debug("Local dirlist: %s" % str(dirlist))
        # retobj['dirlist'] = dirlist

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

            if not dataset or not dataset.get('id'):
                # abort updating
                log.debug("Package '%s' not found" % package_dict.get('name'))
                raise NotFound("Package '%s' not found" % package_dict.get('name'))

            log.debug("Using existing package with id %s" % str(dataset.get('id')))

        except NotFound as e:
            # -----------------------------------------------------------------------
            # create the package dictionary instead
            # -----------------------------------------------------------------------

            # add the metadata from the harvester
            package_dict = self._add_harvester_metadata(package_dict, context)

            # version
            if not package_dict.get('resources'):
                package_dict['version'] = now

            # title of the package
            if not package_dict.get('title'):
                package_dict['title'] = {
                    "de": self.remotefolder.title(),
                    "en": self.remotefolder.title(),
                    "fr": self.remotefolder.title(),
                    "it": self.remotefolder.title()
                }
            # for DCAT schema - same info as in the title
            if not package_dict.get('display_name'):
                package_dict['display_name'] = package_dict['title']

            package_dict['creator_user_id'] = model.User.get(context['user']).id

            # fill with defaults
            for key in ['issued', 'modified', 'metadata_created', 'metadata_modified']:
                if not package_dict.get(key):
                    package_dict[key] = now
            for key in ['resources', 'groups', 'tags', 'extras', 'contact_points', 'relations', 'relationships_as_object', 'relationships_as_subject', 'publishers', 'see_alsos', 'temporals']:
                if not package_dict.get(key):
                    package_dict[key] = []
            for key in ['keywords']:
                if not package_dict.get(key):
                    package_dict[key] = {}

            if not package_dict.get('language'):
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

            # -----------------------------------------------------------------------
            # create the package
            # -----------------------------------------------------------------------

            log.debug("Package dict (pre-creation): %s" % str(package_dict))

            # This logic action requires to call check_access to 
            # prevent the Exception: 'Action function package_show did not call its auth function'
            # Adds action name onto the __auth_audit stack
            result = check_access('package_create', context)
            if not result:
                self._save_object_error('%s not authorised to create packages (object %s)' % (self.harvester_name, harvest_object.id), harvest_object, stage)
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
            self._save_object_error('Could not update or create package: %s' % self.harvester_name, harvest_object, stage)
            return False


        # TODO
        # associate the harvester with the dataset
        harvest_object.guid = dataset['id']
        harvest_object.package_id = dataset['id']



        # =======================================================================
        # resources
        # =======================================================================

        log.debug('Importing file: %s' % str(file))

        site_url = ckanconf.get('ckan.site_url', None)
        if not site_url:
            self._save_object_error('Could not get site_url from CKAN config file', harvest_object, stage)
            return False


        # import the the file into CKAN
        # ---------------------------
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
            # set mime types
            file_format = mimetype = mimetype_inner = ext
            # TODO: find out what the inner mimetype is inside of archives
            # if ext in ['ZIP', 'RAR', 'TAR', 'TAR.GZ', '7Z']:
                # mimetype_inner = 'TXT'
                # pass


        resource_meta = None

        # ------------------------------------------------------------
        # CLEAN-UP the dataset:
        # Before we upload this resource, we search for 
        # an old resource by this (munged) filename.
        # If a resource is found, we delete it.
        # A revision of the resource will be kept by CKAN.
        if dataset.get('resources') and len(dataset['resources']):
            # Find resource in the existing packages resource list
            for res in dataset['resources']:
                # match the resource by its filename
                if os.path.basename(res.get('url')) != munge_name(os.path.basename(file)):
                    continue
                # ignore deleted resources
                if res.get('status') != 'active':
                    continue
                try:
                    # store the resource's metadata for later use
                    resource_meta = res
                    # delete this resource
                    allowed = check_access('resource_delete', context)
                    if not allowed:
                        self._save_object_error('%s not authorised to delete resource (object %s)' % (self.harvester_name, harvest_object.id), harvest_object, stage)
                        return False
                    # delete the resource
                    get_action('resource_delete')(context, {'id': res.get('id')})
                    log.debug("Deleted resource %s" % res.get('id'))
                    # remove this resource from the data dict
                    dataset['resources'].remove(res)
                    # there should only be one file with the same name in each dataset
                    break
                except Exception as e:
                    log.error("Error deleting the existing resource %s: %s" % (str(res.get('id'), str(e))))
                    pass
        # ------------------------------------------------------------


        # use remote API to upload the file
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

                # take some globally defined default values for the resource from the harvester
                if self.resource_dict_meta:
                    resource_meta = self.resource_dict_meta
                else:
                    resource_meta = {}

                resource_meta['package_id'] = dataset['id']

                resource_meta['identifier'] = os.path.basename(file)

                resource_meta['issued'] = now
                resource_meta['modified'] = now

                # TODO
                resource_meta['language'] = 'en'
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

                # ----------------------------------------------------

                resource_meta['name'] = json.dumps({
                        "de": os.path.basename(file),
                        "en": os.path.basename(file),
                        "fr": os.path.basename(file),
                        "it": os.path.basename(file)
                    })
                resource_meta['title'] = resource_meta['name']

                resource_meta['description'] = json.dumps({
                        "de": "",
                        "en": "",
                        "fr": "",
                        "it": ""
                    })

                log_msg = "Creating new resource: %s"

            # -----------------------------------------------------
            # create the resource, but use the metadata of the old resource
            # -----------------------------------------------------
            else:

                # a resource was found - use the existing metadata

                # the resource should get a new id, so delete the old one
                # the resource will get a new revision id
                for key in ['id', 'revision_id']:
                    try:
                        del resource_meta[key]
                    except KeyError as e:
                        pass

                log_msg = "Creating resource (with known metadata): %s"


            # url parameter is ignored for resource uploads, but required by ckan
            resource_meta['url'] = 'http://dummy-value'

            # TODO
            resource_meta['download_url'] = 'http://dummy-value'

            if size != None:
                resource_meta['size'] = size
                resource_meta['byte_size'] = size / 8 # TODO

            log.info(log_msg % str(resource_meta))

            # upload with requests library to avoid ckanapi
            # ---------------------------------------------------------------------
            api_url = site_url + self._get_action_api_offset() + '/resource_create'
            apikey = model.User.get(context['user']).apikey.encode('utf8')
            log.debug("Posting to api_url: %s" % str(api_url))
            headers = {
                'Authorization': apikey,
                'X-CKAN-API-Key': apikey,
                'user-agent': 'ftp-harvester/1.0.0'
                # 'Content-Type': 'multipart/form-data', # http://stackoverflow.com/a/18590706/426266
            }
            r = requests.post(api_url, data=resource_meta, files={'file': fp}, headers=headers)
            log.debug('Response: %s' % r.text)
            if r.status_code != 200:
                # raise Exception(str(resource_meta))
                r.raise_for_status()
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


        # TODO:
        # the last harvest job needs to clean and remove the tmpfolder
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
