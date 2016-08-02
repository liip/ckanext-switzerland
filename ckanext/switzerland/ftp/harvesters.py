"""
FTP Harvesters
"""


import logging

from BaseFTPHarvester import BaseFTPHarvester

log = logging.getLogger(__name__)


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

    # update frequency (in hours)
    frequency = 24

    # package metadata
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

    # whether or not to unzip the files found locally
    do_unzip = True  # PROD: set this to True


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

    # update frequency (in hours)
    frequency = 4

    # package metadata
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

    # whether or not to unzip the files found locally
    do_unzip = True  # no zip files in the folder (so far)
