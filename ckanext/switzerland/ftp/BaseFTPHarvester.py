'''
CKAN FTP Harvester

A Harvesting Job is performed in two phases. In first place, the
**gather** stage collects all the Ids and URLs that need to be fetched
from the harvest source. Errors occurring in this phase
(``HarvestGatherError``) are stored in the ``harvest_gather_error``
table.
During the next phase, the **fetch** stage retrieves the
``HarvestedObjects`` and, if necessary, the **import** stage stores
them on the database. Errors occurring in this second stage
(``HarvestObjectError``) are stored in the ``harvest_object_error``
table.
'''

import urllib2

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_name
from simplejson.scanner import JSONDecodeError

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

import logging
log = logging.getLogger(__name__)

from base import HarvesterBase

from pylons import config as ckanconf

import sys, os, json
import errno
import subprocess
import zipfile

import ftplib

from ckan.logic.action.create import package_create, group_create, organization_create
from ckan.logic.action.update import package_update, group_update, organization_update
from ckan.logic.action.get import package_show

import pprint # debug-only



# ----------------------------------------------------
# host = "THEHOST.com"
# port = 22
# password = "THEPASSWORD"
# username = "THEUSERNAME"
# remotedirectory = "./THETARGETDIRECTORY/"
# localpath = "./LOCALDIRECTORY/"
# ----------------------------------------------------



class FTPHelper(object):

    _config = None

    ftps = None

    remotefolder = ''

    def __init__(self, remotefolder=''):

        # load the ftp configuration from ckan config file
        ftpconfig = {}
        for key in ['username', 'password', 'host', 'port', 'remotedirectory', 'localpath']:
            ftpconfig[key] = ckanconf.get('ckan.ftp.%s' % key, '')
        ftpconfig['host'] = str(ftpconfig['host'])
        ftpconfig['port'] = int(ftpconfig['port'])
        self._config = ftpconfig

        self.remotefolder = remotefolder.rstrip("/")

        # create the local directory, if it does not exist
        # TODO: use Python temp lib
        self.create_local_dir()

    def __enter__(self):
        # establish ftp connection
        self._connect()
        # return helper
        return self

    def __exit__(self, type, value, traceback):
        # disconnect ftp
        self._disconnect()

    def get_top_folder(self):
        return "%s:%d" % (self._config['host'], self._config['port'])

    def _mkdir_p(self, path, perms=0777):
        """
        Recursively create local directories
        See: http://stackoverflow.com/a/600612/426266
        """
        try:
            os.makedirs(path, perms)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                # path already exists
                pass
            else:
                # something went wrong with the creation of the directories
                raise

    def create_local_dir(self, folder=None):
        """
        Create a local folder

        @type  folder: string
        @param folder: Name of the folder
        @rtype:   None
        @return:  None
        """
        if not folder:
            folder = self._config['localpath']
        # create the local directory if it does not exist

        folder = folder.rstrip("/")

        if not os.path.isdir(folder):
            log.debug("Creating folder: %s" % str(folder))
            self._mkdir_p(folder)

    def _connect(self):
        """
        Establish an FTP connection

        @rtype:   None
        @return:  None
        """
        # overwrite the default port (21)
        ftplib.FTP.port = int(self._config['port'])
        # connect
        self.ftps = ftplib.FTP_TLS(self._config['host'], self._config['username'], self._config['password'])
        # switch to secure data connection
        self.ftps.prot_p()

    def _disconnect(self):
        """
        Close ftp connection

        @rtype:   None
        @return:  None
        """
        self.ftps.quit() # '221 Goodbye.'

    def cdremote(self, remotedir=None):
        """
        Change remote directory

        @type  remotedir: string
        @param remotedir: Full path on the remote server
        @rtype:   None
        @return:  None
        """
        if not remotedir:
            remotedir = self.remotefolder # self._config['remotedirectory']
        self.ftps.cwd(remotedir)

    def get_remote_dirlist(self, folder=None):
        """
        List files and sub-directories in the current directory

        @rtype:   list
        @return:  Directory listing (exclusing '.' and '..')
        """
        # get dir listing of a specific directory
        if folder:
            dirlist = self.ftps.nlst(folder)
        # get dir listing of current directory
        else:
            dirlist = self.ftps.nlst()
        # filter out '.' and '..' and return the list
        return filter(lambda x: x not in ['.', '..'], dirlist)

    # see: http://stackoverflow.com/a/31512228/426266
    def get_remote_dirlist_all(self, folder=None):
        if not folder:
            folder = self.remotefolder
        dirs = []
        new_dirs = self.get_remote_dirlist(folder)
        while len(new_dirs) > 0:
            for dir in new_dirs:
                dirs.append(dir)
            old_dirs = new_dirs[:]
            new_dirs = []
            for dir in old_dirs:
                for new_dir in self.get_remote_dirlist(dir):
                    new_dirs.append(new_dir)
        dirs.sort()
        return dirs

    def is_empty_dir(self, folder=None):
        """
        Check if a remote directory is empty

        @rtype:   list
        @return:  Directory listing (exclusing '.' and '..')
        """
        # get dir listing of a specific directory
        if folder:
            num_files = len(self.get_dirlist(folder))
        else:
            num_files = len(self.get_dirlist())
        return num_files

    def wget_folder_fetch(self):
        # optional parameters:
            # -nv: non-verbose
            # --no-clobber: do not overwrite existing files
        return subprocess.call(
            "/usr/local/bin/wget -r --no-clobber --ftp-user='%s' --ftp-password='%s' -np --no-check-certificate ftps://%s:%d/%s" % (
                self._config['username'],
                self._config['password'],
                self._config['host'],
                int(self._config['port']),
                os.path.join(self._config['remotedirectory'], self.remotefolder)
            ), shell=True)

    def wget_fetch(self, file):
        return subprocess.call(
            "/usr/local/bin/wget --no-clobber --ftp-user='%s' --ftp-password='%s' -np --no-check-certificate ftps://%s:%d/%s" % (
                self._config['username'],
                self._config['password'],
                self._config['host'],
                int(self._config['port']),
                file
            ), shell=True)

    def fetch(self, filename, localpath=None):
        """
        Fetch a single file from the remote server

        @type  filename: string
        @param filename: Name of the file to download
        @rtype:   string
        @return:  FTP status message
        """
        if not localpath:
            localpath = os.path.join(self._config['localpath'], filename)

        localfile = open(localpath, 'wb')
        status = self.ftps.retrbinary('RETR %s' % filename, localfile.write)
        localfile.close()
        # TODO: check status
        # TODO: verify download
        return status

    # TODO
    # def getdir(self, path='', recursive=True):
    #     """
    #     Retrieve all files of a directory, optionally including all sub-directories
    #     @type  remotedir: string
    #     @param remotedir: Full path on the remote server
    #     @rtype:   None
    #     @return:  None
    #     """
    #     # import pprint
    #     # pprint.pprint(e)
    #     # print "FILE DOWNLOAD EXCP: %s" % e
    #     # cd into the remote directory
    #     self.cdremote(path)
    #     # Return a list containing the names of the entries in the given path.
    #     # The list is in arbitrary order. It does not include the special entries '.' and '..' even if they are present in the folder.
    #     dirlist = self.listdir() # 226 Transfer complete
    #     for item in dirlist:
    #         try:
    #             # try to download the file
    #             self.fetch(item)
    #         except Exception as e:
    #             # it must be a directory
    #             if recursive:
    #                 self.getdir(item)
    #         # prefix the filename
    #         # fileordir = os.path.join(path, fileordir)




class BaseFTPHarvester(HarvesterBase):
    """
    A FTP Harvester for data
    """

    config = None # ckan harvester config, not ftp config

    api_version = 2
    action_api_version = 3

    # default harvester id, to be overwritten by child classes
    harvester_name = 'ckanftp'

    tmpfile_extension = '.TMP'

    # default remote directory to harvest, to be overwritten by child classes
    # e.g. infodoc or didok
    remotefolder = ''

    # subfolder in the above remote folder
    environment = 'test'

    do_unzip = True


    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version
    def _get_action_api_offset(self):
        return '/api/%d/action' % self.action_api_version
    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version


    def _unzip(self, filepath):
        """
        Extract a single zip file
        E.g. will extract a file /tmp/somedir/myfile.zip into /tmp/somedir/
        """
        target_folder = os.path.dirname(filepath)
        zfile = zipfile.ZipFile(filepath)
        zfile.extractall(target_folder)

    def _get_local_dirlist(self, localpath="."):
        """
        Get directory listing, including all sub-folders
        """
        dirlist = []
        for dirpath, dirnames, filenames in os.walk(localpath):
            for filename in [f for f in filenames]:
                dirlist.append(os.path.join(dirpath, filename))

        # .TMP must be ignored, as they are still being uploaded
        dirlist =  filter(lambda x: not x.lower().endswith(self.tmpfile_extension), dirlist)

        return dirlist

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])
            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': '%sharvest' % self.harvester_name.lower(), # 'ckanftp'
            'title': 'CKAN FTP %s Harvester' % self.harvester_name,
            'description': 'Fetches %s/%s' % (self.remotefolder, self.environment),
            'form_config_interface': 'Text'
        }

    def validate_config(self, config):

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
                if not isinstance(config_obj['default_tags'],list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'],list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'],dict):
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
                    if not isinstance(config_obj[key],bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError,e:
            raise e

        return config


    def gather_stage(self, harvest_job):
        """
        Dummy stage that launches the next phase

        :param harvest_job: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """
        log.debug('In %s FTPHarvester gather_stage' % self.harvester_name) # harvest_job.source.url

        # fake a harvest object for this package to start the next step
        harvest_object = HarvestObject(guid=self.harvester_name, job=harvest_job)
        harvest_object.save()
        return [ harvest_object.id ]




        # old stuff

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


    def fetch_stage(self, harvest_object):
        """
        Fetching of resources

        :param harvest_object: HarvestObject
        :returns: True|None Whether HarvestObject was saved or not
        """
        log.debug('In %s FTPHarvester fetch_stage' % self.harvester_name)

        # self._set_config(harvest_job.source.config)

        remotefolder = os.path.join('/', self.remotefolder, self.environment) # e.g. didok/test
        log.debug("FTP remotefolder: %s" % remotefolder)

        # return dict
        retobj = {}

        try:

            with FTPHelper(remotefolder) as ftph:

                # cd into the remote directory
                ftph.cdremote()

                # get a directory listing
                # dirlist = ftph.get_dirlist()

                # store some config for the next step
                retobj['topfolder'] = ftph.get_top_folder() # 'ftp-secure.sbb.ch:990'

                # change into the local directory
                os.chdir(ftph._config['localpath'])

                # fetch the files via wget (TBD)
                cmdstatus = ftph.wget_folder_fetch()
                log.debug("wget_folder_fetch cmdstatus: %s" % str(cmdstatus))
                if cmdstatus > 0:
                    raise CmdError("WGet exited with status %d" % cmdstatus)

                # save the folder name where the files were downloaded
                retobj['workingdir'] = os.path.join(ftph._config['localpath'], retobj['topfolder'])

        except ftplib.all_errors as e:
            self._save_object_error('Ftplib error: %s' % str(e), harvest_object)
            return None

        except CmdError as e:
            self._save_object_error('Cmd error: %s' % str(e), harvest_object, 'Fetch')
            return None

        except subprocess.CalledProcessError as e:
            self._save_object_error('WGet Error [%d]: %s' % (e.returncode, e), harvest_object, 'Fetch')
            return None

        # except Exception as e:
        #     self._save_object_error('An error occurred: %s' % e, harvest_object, 'Fetch')
        #     return None


        # get an updated list of all local files (extracted and zip)
        dirlist = self._get_local_dirlist()

        if not len(dirlist):
            self._save_object_error('No files found to harvest in remote directory [%s]. Harvest aborted.' % str(remotefolder), harvest_object, 'Fetch')
            return None

        # log.debug("Unzipping files")
        if self.do_unzip:
            for file in dirlist:
                # if file is a zip, unzip
                filename, file_extension = os.path.splitext(file)
                if file_extension == '.zip':
                    log.debug("Unzipping: %s" % file)
                    self._unzip(file)

        # get an updated list of all local files (extracted and zip)
        # dirlist = self._get_local_dirlist()
        # log.debug("Fetched files: %s" % str(dirlist))
        # retobj['dirlist'] = dirlist

        # Save the directory listing and other info in the HarvestObject
        harvest_object.content = json.dumps(retobj)
        harvest_object.save()

        return True


    def import_stage(self, harvest_object):
        """
        Importing the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the HarvestObject was imported or not
        """
        log.debug('In %s FTPHarvester import_stage' % self.harvester_name) # harvest_job.source.url

        if not harvest_object:
            log.error('No harvest object received')
            return False
        if not harvest_object.content:
            self._save_object_error('Empty content for harvest object %s' % harvest_object.id, harvest_object, 'Import')
            return False

        context = {'model': model, 'session': Session, 'user': self._get_user_name()}

        # api key of the harvester user
        harvest_api_key = model.User.get(context['user']).apikey.encode('utf8')

        # set harvester config
        self._set_config(harvest_object.job.source.config)

        # unserialize the info from the previous step
        harvest_object.content = json.loads(harvest_object.content)

        # need the local path
        if not harvest_object.content.get('workingdir'):
            self._save_object_error('Missing workingdir in harvest object %s' % harvest_object.id, harvest_object, 'Import')
            return False

        wdir = os.path.join(harvest_object.content['workingdir'], self.remotefolder, self.environment)

        log.debug('Workingdir: %s' % str(wdir))

        # change into the local directory
        os.chdir(wdir) # e.g. /tmp/ftpharvest/ftp.somedomain.com:21/infoplus/prod


        dirlist = self._get_local_dirlist()

        if not len(dirlist):
            self._save_object_error('No files found to process for %s harvester (object %s)' % (self.harvester_name, harvest_object.id), harvest_object, 'Import')
            return False


        try:

            # =======================================================================
            # create a package dictionary
            # =======================================================================
            package_dict = {}
            package_dict['name'] = self.remotefolder # self._ensure_name_is_unique(os.path.basename(self.remotefolder))

            # -----------------------------------------------------------------------
            # Set default tags if needed
            # -----------------------------------------------------------------------
            default_tags = self.config.get('default_tags', [])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend([t for t in default_tags if t not in package_dict['tags']])

            # -----------------------------------------------------------------------
            # group creation
            # -----------------------------------------------------------------------
            # remote_groups = self.config.get('remote_groups', None)
            # if not remote_groups in ('only_local', 'create'):
            #     # Ignore remote groups
            #     package_dict.pop('groups', None)
            # else:
            #     if not 'groups' in package_dict:
            #         package_dict['groups'] = []
            #     # check if remote groups exist locally, otherwise remove
            #     validated_groups = []
            #     for group_name in package_dict['groups']:
            #         try:
            #             data_dict = {'id': group_name}
            #             group = get_action('group_show')(context, data_dict)
            #             if self.api_version == 1:
            #                 validated_groups.append(group['name'])
            #             else:
            #                 validated_groups.append(group['id'])
            #         except NotFound, e:
            #             log.info('Group %s is not available' % group_name)
            #             if remote_groups == 'create':
            #                 try:
            #                     group = self._get_group(harvest_object.source.url, group_name)
            #                 except RemoteResourceError:
            #                     log.error('Could not get remote group %s' % group_name)
            #                     continue
            #                 for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
            #                     group.pop(key, None)
            #                 get_action('group_create')(context, group)
            #                 log.info('Group %s has been newly created' % group_name)
            #                 if self.api_version == 1:
            #                     validated_groups.append(group['name'])
            #                 else:
            #                     validated_groups.append(group['id'])
            #     package_dict['groups'] = validated_groups

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []
                package_dict['groups'].extend([g for g in default_groups if g not in package_dict['groups']])

            # -----------------------------------------------------------------------
            # organization creation
            # -----------------------------------------------------------------------
            #     # Local harvest source organization
            #     source_dataset = get_action('package_show')(context, {'id': harvest_object.source.id})
            #     local_org = source_dataset.get('owner_org')
            #     remote_orgs = self.config.get('remote_orgs', None)
            #     if not remote_orgs in ('only_local', 'create'):
            #         # Assign dataset to the source organization
            #         package_dict['owner_org'] = local_org
            #     else:
            #         if not 'owner_org' in package_dict:
            #             package_dict['owner_org'] = None
            #         # check if remote org exist locally, otherwise remove
            #         validated_org = None
            #         remote_org = package_dict['owner_org']
            #         if remote_org:
            #             try:
            #                 data_dict = {'id': remote_org}
            #                 org = get_action('organization_show')(context, data_dict)
            #                 validated_org = org['id']
            #             except NotFound, e:
            #                 log.info('Organization %s is not available' % remote_org)
            #                 if remote_orgs == 'create':
            #                     try:
            #                         try:
            #                             org = self._get_organization(harvest_object.source.url, remote_org)
            #                         except RemoteResourceError:
            #                             # fallback if remote CKAN exposes organizations as groups
            #                             # this especially targets older versions of CKAN
            #                             org = self._get_group(harvest_object.source.url, remote_org)
            #                         for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
            #                             org.pop(key, None)
            #                         get_action('organization_create')(context, org)
            #                         log.info('Organization %s has been newly created' % remote_org)
            #                         validated_org = org['id']
            #                     except (RemoteResourceError, ValidationError):
            #                         log.error('Could not get remote org %s' % remote_org)
            #         package_dict['owner_org'] = validated_org or local_org

            # -----------------------------------------------------------------------
            # extras creation
            # -----------------------------------------------------------------------

            #     # Find any extras whose values are not strings and try to convert
            #     # them to strings, as non-string extras are not allowed anymore in
            #     # CKAN 2.0.
            #     for key in package_dict['extras'].keys():
            #         if not isinstance(package_dict['extras'][key], basestring):
            #             try:
            #                 package_dict['extras'][key] = json.dumps(
            #                         package_dict['extras'][key])
            #             except TypeError:
            #                 # If converting to a string fails, just delete it.
            #                 del package_dict['extras'][key]

            # Set default extras if needed
            default_extras = self.config.get('default_extras',{})
            if default_extras:
                override_extras = self.config.get('override_extras',False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = {}
                for key,value in default_extras.iteritems():
                    if not key in package_dict['extras'] or override_extras:
                        # Look for replacement strings
                        if isinstance(value,basestring):
                            value = value.format(harvest_source_id=harvest_object.job.source.id,
                                     harvest_source_url=harvest_object.job.source.url.strip('/'),
                                     harvest_source_title=harvest_object.job.source.title,
                                     harvest_job_id=harvest_object.job.id,
                                     harvest_object_id=harvest_object.id,
                                     dataset_id=package_dict['id'])

                        package_dict['extras'][key] = value


            log.debug("Package dict (pre-save): %s" % str(package_dict))


            # -----------------------------------------------------------------------
            # create or update package
            # -----------------------------------------------------------------------

            # check if the package already exists
            try:

                existing_package = package_show(context, {'id': package_dict.get('name')})
                package_dict['id'] = existing_package.get('id')

                if not package_dict['id']:
                    # abort updating
                    log.debug("Package '%s' not found" % package_dict.get('name'))
                    raise NotFound("Package '%s' not found" % package_dict.get('name'))

                log.debug("Found package with id %s" % str(package_dict['id']))

                # patch the package dict
                existing_package.update(package_dict)

                # update the package
                # requires the id
                # see: https://github.com/ckan/ckanext-harvest/blob/7f506913f8e78988899af9c1dd518dc76e2c3e62/ckanext/harvest/harvesters/base.py#L219
                # result = self._create_or_update_package(package_dict, harvest_object)

                # update the package
                dataset = package_update(context, existing_package)
                log.debug("Updated: %s" % str(dataset.get('name')))

            # TODO
            except NotFound as e:
                # create the package instead
                dataset = package_create(context, package_dict)
                log.info("Created package: %s" % str(dataset['name']))

            except Exception as e:
                self._save_object_error('Package update/creation error: %s' % str(e), harvest_object, 'Import')
                return False


            # -----------------------------------------------------------------------
            # Configure permissions of the package
            # -----------------------------------------------------------------------
            if dataset:

                package = model.Package.get(package_dict['id'])

                # Clear default permissions
                model.clear_user_roles(package)

                # Setup harvest user as admin of this package
                user_name = self.config.get('user', u'harvest')
                user = model.User.get(user_name)
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

                # Set the package read-only ?
                if self.config.get('read_only', True) is True:
                    # Other users can only read
                    for user_name in (u'visitor', u'logged_in'):
                        user = model.User.get(user_name)
                        pkg_role = model.PackageRole(package=package, user=user, role=model.Role.READER)


            # -----------------------------------------------------------------------
            # resources creation
            # -----------------------------------------------------------------------

            # now that the harvest user is admin of package, 

            log.debug('Processing files: %s' % str(dirlist))

            # import the local files into CKAN
            for file in dirlist:


                log.debug("TODO: add %s" % str(file))


                # TODO: use API to upload


                # TODO: add resources



                # TODO: import metadata of files


                # TODO: add resources to package_dict


                pass


            # -----------------------------------------------------------------------
            # return result
            # -----------------------------------------------------------------------

        except ValidationError, e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
            return False

        except Exception, e:
            self._save_object_error('%r' % e, harvest_object, 'Import')
            return False


        # =======================================================================
        return True


class ContentFetchError(Exception):
    pass

class RemoteResourceError(Exception):
    pass

class CmdError(Exception):
    pass
