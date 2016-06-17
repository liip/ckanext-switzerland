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

import paramiko
import sys, os
import ckan.config as ftpconfig



# ----------------------------------------------------
# host = "THEHOST.com"                      # TODO: This should be part of the harvester's source url
# port = 22
# password = "THEPASSWORD"
# username = "THEUSERNAME"
# remotedirectory = "./THETARGETDIRECTORY/" # TODO: This should be part of the harvester's source url
# localpath = "./LOCALDIRECTORY/"
# ----------------------------------------------------




class FTPHarvester(HarvesterBase):
    """
    A FTP Harvester for data
    """
    config = None

    api_version = 2
    action_api_version = 3

    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version

    def _get_action_api_offset(self):
        return '/api/%d/action' % self.action_api_version

    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version

    def info(self):
        return {
            'name': 'ckanftp',
            'title': 'CKAN FTP Harvester',
            'description': 'Fetches FTP data',
            'form_config_interface':'Text'
        }

    def validate_config(self, config):

        # if not config:
        #     return config

        # try:
        #     config_obj = json.loads(config)

        #     if 'api_version' in config_obj:
        #         try:
        #             int(config_obj['api_version'])
        #         except ValueError:
        #             raise ValueError('api_version must be an integer')

        #     if 'default_tags' in config_obj:
        #         if not isinstance(config_obj['default_tags'],list):
        #             raise ValueError('default_tags must be a list')

        #     if 'default_groups' in config_obj:
        #         if not isinstance(config_obj['default_groups'],list):
        #             raise ValueError('default_groups must be a list')

        #         # Check if default groups exist
        #         context = {'model':model,'user':c.user}
        #         for group_name in config_obj['default_groups']:
        #             try:
        #                 group = get_action('group_show')(context,{'id':group_name})
        #             except NotFound,e:
        #                 raise ValueError('Default group not found')

        #     if 'default_extras' in config_obj:
        #         if not isinstance(config_obj['default_extras'],dict):
        #             raise ValueError('default_extras must be a dictionary')

        #     if 'user' in config_obj:
        #         # Check if user exists
        #         context = {'model':model,'user':c.user}
        #         try:
        #             user = get_action('user_show')(context,{'id':config_obj.get('user')})
        #         except NotFound,e:
        #             raise ValueError('User not found')

        #     for key in ('read_only','force_all'):
        #         if key in config_obj:
        #             if not isinstance(config_obj[key],bool):
        #                 raise ValueError('%s must be boolean' % key)

        # except ValueError,e:
        #     raise e

        return config


    def gather_stage(self, harvest_job):
        """
        Gathers resources to fetch

        :param harvest_job: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """
        log.debug('In FTPHarvester gather_stage (%s)' % harvest_job.source.url)

        # fake a harvest object and return it for the next step
        obj = HarvestObject(guid='ftpdata', job=harvest_job)
        obj.save()

        return [ obj.id ]


        # old stuff

        # get_all_packages = True
        # package_ids = []

        # self._set_config(harvest_job.source.config)

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
        log.debug('In FTPHarvester fetch_stage')

        # create the directory if it does not exist
        if not os.path.isdir(ftpconfig.localpath):
            os.mkdir(ftpconfig.localpath, 0755);

        # # fetch the files via SFTP
        # transport = paramiko.Transport((ftpconfig.host, ftpconfig.port))
        # # transport.connect(username=ftpconfig.username, password=ftpconfig.password)
        # transport.connect(username='', password=None, pkey=None)
        # sftp = paramiko.SFTPClient.from_transport(transport)
        # try:
        #     # Return a list containing the names of the entries in the given path.
        #     # The list is in arbitrary order. It does not include the special entries '.' and '..' even if they are present in the folder.
        #     dirlist = sftp.listdir(ftpconfig.remotedirectory)
        #     if len(dirlist):
        #         for file_id in dirlist:
        #             # fetch the file
        #             sftp.get(ftpconfig.remotedirectory, ftpconfig.localpath)
        # except:
        #        return None
        # sftp.close()
        # transport.close()


        # fetch the files via FTP
        from ftplib import FTP
        try:
            ftp = FTP(ftpconfig.host)
            ftp.login()
        except:
            throw "FTP connection error" # TODO - CKAN exception
        try:
            ftp.cwd(ftpconfig.remotedirectory)
            dirlist = ftp.retrlines('LIST')
            if len(dirlist):
                for file in dirlist:
                    # fetch the file
                    ftp.retrbinary( 'RETR %s' % file, open(os.path.join(ftpconfig.localpath, file), 'wb').write )
        except:
           return None # TODO


        # Save the filelist in the HarvestObject so that it can be processed in the next step
        harvest_object.content = dirlist
        harvest_object.save()

        return True



        # old stuff

        # self._set_config(harvest_object.job.source.config)

        # # Get source URL
        # url = harvest_object.source.url.rstrip('/')
        # url = url + self._get_rest_api_offset() + '/package/' + harvest_object.guid

        # # Get contents
        # try:
        #     content = self._get_content(url)
        # except ContentFetchError, e:
        #     self._save_object_error('Unable to get content for package: %s: %r' % \
        #                                 (url, e),harvest_object)
        #     return None

        # # Save the fetched contents in the HarvestObject
        # harvest_object.content = content
        # harvest_object.save()
        # return True


    def import_stage(self, harvest_object):
        """
        Importing the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the HarvestObject was imported or not
        """
        log.debug('In FTPHarvester import_stage')

        # there should be a harvest object and filelist from the previous step
        if not harvest_object:
            log.error('No harvest object received')
            return False
        if not harvest_object.get('content'):
            log.error('Harvest object is missing file list')
            return False

        # get the file list from the Harvest object
        dirlist = harvest_object.content

        # TODO: import the local files into CKAN

        # TODO: map the files to organisations and groups















        # old stuff

        # context = {'model': model, 'session': Session, 'user': self._get_user_name()}
        # if not harvest_object:
        #     log.error('No harvest object received')
        #     return False

        # if harvest_object.content is None:
        #     self._save_object_error('Empty content for object %s' % harvest_object.id,
        #             harvest_object, 'Import')
        #     return False

        # self._set_config(harvest_object.job.source.config)

        # try:
        #     package_dict = json.loads(harvest_object.content)

        #     if package_dict.get('type') == 'harvest':
        #         log.warn('Remote dataset is a harvest source, ignoring...')
        #         return True

        #     # Set default tags if needed
        #     default_tags = self.config.get('default_tags',[])
        #     if default_tags:
        #         if not 'tags' in package_dict:
        #             package_dict['tags'] = []
        #         package_dict['tags'].extend([t for t in default_tags if t not in package_dict['tags']])

        #     remote_groups = self.config.get('remote_groups', None)
        #     if not remote_groups in ('only_local', 'create'):
        #         # Ignore remote groups
        #         package_dict.pop('groups', None)
        #     else:
        #         if not 'groups' in package_dict:
        #             package_dict['groups'] = []

        #         # check if remote groups exist locally, otherwise remove
        #         validated_groups = []

        #         for group_name in package_dict['groups']:
        #             try:
        #                 data_dict = {'id': group_name}
        #                 group = get_action('group_show')(context, data_dict)
        #                 if self.api_version == 1:
        #                     validated_groups.append(group['name'])
        #                 else:
        #                     validated_groups.append(group['id'])
        #             except NotFound, e:
        #                 log.info('Group %s is not available' % group_name)
        #                 if remote_groups == 'create':
        #                     try:
        #                         group = self._get_group(harvest_object.source.url, group_name)
        #                     except RemoteResourceError:
        #                         log.error('Could not get remote group %s' % group_name)
        #                         continue

        #                     for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
        #                         group.pop(key, None)

        #                     get_action('group_create')(context, group)
        #                     log.info('Group %s has been newly created' % group_name)
        #                     if self.api_version == 1:
        #                         validated_groups.append(group['name'])
        #                     else:
        #                         validated_groups.append(group['id'])

        #         package_dict['groups'] = validated_groups


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

        #     # Set default groups if needed
        #     default_groups = self.config.get('default_groups', [])
        #     if default_groups:
        #         if not 'groups' in package_dict:
        #             package_dict['groups'] = []
        #         package_dict['groups'].extend([g for g in default_groups if g not in package_dict['groups']])

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

        #     # Set default extras if needed
        #     default_extras = self.config.get('default_extras',{})
        #     if default_extras:
        #         override_extras = self.config.get('override_extras',False)
        #         if not 'extras' in package_dict:
        #             package_dict['extras'] = {}
        #         for key,value in default_extras.iteritems():
        #             if not key in package_dict['extras'] or override_extras:
        #                 # Look for replacement strings
        #                 if isinstance(value,basestring):
        #                     value = value.format(harvest_source_id=harvest_object.job.source.id,
        #                              harvest_source_url=harvest_object.job.source.url.strip('/'),
        #                              harvest_source_title=harvest_object.job.source.title,
        #                              harvest_job_id=harvest_object.job.id,
        #                              harvest_object_id=harvest_object.id,
        #                              dataset_id=package_dict['id'])

        #                 package_dict['extras'][key] = value

        #     for resource in package_dict.get('resources', []):
        #         # Clear remote url_type for resources (eg datastore, upload) as
        #         # we are only creating normal resources with links to the
        #         # remote ones
        #         resource.pop('url_type', None)

        #         # Clear revision_id as the revision won't exist on this CKAN
        #         # and saving it will cause an IntegrityError with the foreign
        #         # key.
        #         resource.pop('revision_id', None)

        #     result = self._create_or_update_package(package_dict,harvest_object)

        #     if result is True and self.config.get('read_only', False) is True:

        #         package = model.Package.get(package_dict['id'])

        #         # Clear default permissions
        #         model.clear_user_roles(package)

        #         # Setup harvest user as admin
        #         user_name = self.config.get('user',u'harvest')
        #         user = model.User.get(user_name)
        #         pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

        #         # Other users can only read
        #         for user_name in (u'visitor',u'logged_in'):
        #             user = model.User.get(user_name)
        #             pkg_role = model.PackageRole(package=package, user=user, role=model.Role.READER)

        #     return result
        # except ValidationError,e:
        #     self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
        #             harvest_object, 'Import')
        # except Exception, e:
        #     self._save_object_error('%r'%e, harvest_object, 'Import')


class ContentFetchError(Exception):
    pass

class RemoteResourceError(Exception):
    pass