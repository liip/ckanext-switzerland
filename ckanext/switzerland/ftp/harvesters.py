'''
FTP Harvesters
'''

# from ckan.lib.munge import munge_name

import logging
log = logging.getLogger(__name__)

from BaseFTPHarvester import BaseFTPHarvester
from BaseFTPHarvester import ContentFetchError, RemoteResourceError


class InfoplusHarvester(BaseFTPHarvester):
    """
    An FTP Harvester for Infoplus data
    """

    # name of the harvester
    harvester_name = 'Infoplus'

    # parent folder to use
    environment = 'Test'

    # parent folder of the data on the ftp server
    remotefolder = 'Info+'

    # package metadata
    package_dict_meta = {
        # package privacy
        'private': False,
        'state': 'active',
        'isopen': False,
        # --------------------------------------------------------------------------
        # author and maintainer
        'author': "Author name", # TODO
        'author_email': "author@example.com", # TODO
        'maintainer': "Maintainer name", # TODO
        'maintainer_email': "maintainer@example.com", # TODO
        # license
        'license_id': "other-open", # TODO
        'license_title': "Other (Open)", # TODO
        'rights': "Other (Open)", # TODO
        # owner organisation
        "owner_org": "7dbaad15-597f-499c-9a72-95de38b95cad", # TODO
        # ckan multilang/switzerland custom required fields
        'coverage': "Coverage", # TODO
        'issued': "21.03.2015", # TODO
        # "modified": "21.03.2016", # TODO
        # "metadata_created": "2016-07-05T07:41:28.741265", # TODO
        # "metadata_modified": "2016-07-05T07:43:30.079030", # TODO
        "url": "https://catalog.data.gov/", # TODO
        "spatial": "Spatial", # TODO
        "accrual_periodicity": "", # TODO
        # --------------------------------------------------------------------------
        "description": { # TODO
            "fr": "FR Description",
            "en": "EN Description",
            "de": "DE Description",
            "it": "IT Description"
        },
        # TODO - release notes could go in here - but where do they come from ?
        "notes": { # TODO
            "fr": "...",
            "en": "...",
            "de": "...",
            "it": "..."
        },
        # --------------------------------------------------------------------------
        'groups': [],
        'tags': [],
        'extras': [],
        "language": ["en", "de", "fr", "it"],
        # relations
        "relations": [{}],
        "relationships_as_object": [], # ???
        "relationships_as_subject": [], # ???
        "see_alsos": [], # ???
        # # publisher (TODO)
        "publishers": [{
            "label": "Publisher 1"
        }],
        # # optional groups
        'keywords': { # TODO
            "fr": [],
            "en": [],
            "de": [],
            "it": []
        },
        'contact_points': [{
            "name": "Contact Name",
            "email": "contact@example.com"
        }],
        "temporals": [{ # TODO
            "start_date": "2014-03-21T00:00:00",
            "end_date": "2019-03-21T00:00:00"
        }],
    }

    resource_dict_meta = {
        # 'state': 'active',
        'rights': 'Other (Open)', # TODO
        'license': 'Other (Open)', # TODO
        'coverage': 'Coverage', # TODO
    }


    # whether or not to unzip the files found locally
    do_unzip = False # PROD: set this to True


    # -----------------------------------------------------------------------
    def gather_stage(self, harvest_job):
        """
        Gathers resources to fetch

        :param harvest_job: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """
        return super(InfoplusHarvester, self).gather_stage(harvest_job)
    # -----------------------------------------------------------------------
    def fetch_stage(self, harvest_object):
        """
        Fetches resources

        :param harvest_object: HarvestObject
        :returns: True|None Whether HarvestObject was saved or not
        """
        return super(InfoplusHarvester, self).fetch_stage(harvest_object)
    # -----------------------------------------------------------------------
    def import_stage(self, harvest_object):
        """
        Importing the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the object was imported or not
        """
        return super(InfoplusHarvester, self).import_stage(harvest_object)
    # -----------------------------------------------------------------------

# =======================================================================


class DidokHarvester(BaseFTPHarvester):
    """
    An FTP Harvester for Didok data
    """

    # name of the harvester
    harvester_name = 'Didok'

    # parent folder to use
    environment = 'Test'

    # folder of the data on the ftp server
    remotefolder = 'DiDok'

    # package metadata
    package_dict_meta = {
        # package privacy
        'private': False,
        'state': 'active',
        'isopen': False,
        # --------------------------------------------------------------------------
        # author and maintainer
        'author': "Author name", # TODO
        'author_email': "author@example.com", # TODO
        'maintainer': "Maintainer name", # TODO
        'maintainer_email': "maintainer@example.com", # TODO
        # license
        'license_id': "other-open", # TODO
        'license_title': "Other (Open)", # TODO
        'rights': "Other (Open)", # TODO
        # owner organisation
        "owner_org": "7dbaad15-597f-499c-9a72-95de38b95cad", # TODO
        # ckan multilang/switzerland custom required fields
        'coverage': "Coverage", # TODO
        'issued': "21.03.2015", # TODO
        # "modified": "21.03.2016", # TODO
        # "metadata_created": "2016-07-05T07:41:28.741265", # TODO
        # "metadata_modified": "2016-07-05T07:43:30.079030", # TODO
        "url": "https://catalog.data.gov/", # TODO
        "spatial": "Spatial", # TODO
        "accrual_periodicity": "", # TODO
        # --------------------------------------------------------------------------
        "description": { # TODO
            "fr": "FR Description",
            "en": "EN Description",
            "de": "DE Description",
            "it": "IT Description"
        },
        # TODO - release notes could go in here - but where do they come from ?
        "notes": { # TODO
            "fr": "...",
            "en": "...",
            "de": "...",
            "it": "..."
        },
        # --------------------------------------------------------------------------
        'groups': [],
        'tags': [],
        'extras': [],
        "language": ["en", "de", "fr", "it"],
        # relations
        "relations": [{}],
        "relationships_as_object": [], # ???
        "relationships_as_subject": [], # ???
        "see_alsos": [], # ???
        # # publisher (TODO)
        "publishers": [{
            "label": "Publisher 1"
        }],
        # # optional groups
        'keywords': { # TODO
            "fr": [],
            "en": [],
            "de": [],
            "it": []
        },
        'contact_points': [{
            "name": "Contact Name",
            "email": "contact@example.com"
        }],
        "temporals": [{ # TODO
            "start_date": "2014-03-21T00:00:00",
            "end_date": "2019-03-21T00:00:00"
        }],
    }

    resource_dict_meta = {
        # 'state': 'active',
        'rights': 'Other (Open)', # TODO
        'license': 'Other (Open)', # TODO
        'coverage': 'Coverage', # TODO
    }

    # whether or not to unzip the files found locally
    do_unzip = False # no zip files in the folder (so far)


    # -----------------------------------------------------------------------
    def gather_stage(self, harvest_job):
        """
        Gathers resources to fetch

        :param harvest_job: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """
        return super(DidokHarvester, self).gather_stage(harvest_job)
    # -----------------------------------------------------------------------
    def fetch_stage(self, harvest_object):
        """
        Fetches resources

        :param harvest_object: HarvestObject
        :returns: True|None Whether HarvestObject was saved or not
        """
        return super(DidokHarvester, self).fetch_stage(harvest_object)
    # -----------------------------------------------------------------------
    def import_stage(self, harvest_object):
        """
        Imports the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the object was imported or not
        """
        return super(DidokHarvester, self).import_stage(harvest_object)
    # -----------------------------------------------------------------------
