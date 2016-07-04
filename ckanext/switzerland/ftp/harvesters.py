'''
FTP Harvesters
'''

# from ckan import model
# from ckan.model import Session, Package

from ckan.logic import ValidationError, NotFound

# from ckan.lib.munge import munge_name

from ckanext.harvest.model import HarvestGatherError, HarvestObjectError

import logging
log = logging.getLogger(__name__)

from BaseFTPHarvester import BaseFTPHarvester
from BaseFTPHarvester import ContentFetchError
from BaseFTPHarvester import RemoteResourceError



class InfoplusHarvester(BaseFTPHarvester):
    """
    An FTP Harvester for Infoplus data
    """

    # name of the harvester
    harvester_name = 'Infoplus'

    # parent folder of the data on the ftp server
    remotefolder = 'Info+'

    # subfolder in the above remote folder
    environment = 'Test'

    do_unzip = False # PROD: set this to True

    # -----------------------------------------------------------------------

    def gather_stage(self, harvest_object):
        """
        Gathers resources to fetch

        :param harvest_object: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """

        ret = super(InfoplusHarvester, self).gather_stage(harvest_object)

        return ret

    # -----------------------------------------------------------------------

    def fetch_stage(self, harvest_object):
        """
        Fetching of resources

        :param harvest_object: HarvestObject
        :returns: True|None Whether HarvestObject was saved or not
        """

        ret = super(InfoplusHarvester, self).fetch_stage(harvest_object)

        return ret

    # -----------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """
        Importing the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the HarvestObject was imported or not
        """

        # harvest_api_key = model.User.get(context['user']).apikey.encode('utf8')

        ret = super(InfoplusHarvester, self).import_stage(harvest_object)

        return ret


# =======================================================================


class DidokHarvester(BaseFTPHarvester):
    """
    An FTP Harvester for Didok data
    """

    # name of the harvester
    harvester_name = 'Didok'

    # parent folder of the data on the ftp server
    remotefolder = 'DiDok'

    # subfolder in the above remote folder
    environment = 'Test'

    do_unzip = False # no zip files in the folder (so far)

    # -----------------------------------------------------------------------

    def gather_stage(self, harvest_object):
        """
        Gathers resources to fetch

        :param harvest_object: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """

        ret = super(InfoplusHarvester, self).gather_stage(harvest_object)

        return ret

    # -----------------------------------------------------------------------

    def fetch_stage(self, harvest_object):
        """
        Fetching of resources

        :param harvest_object: HarvestObject
        :returns: True|None Whether HarvestObject was saved or not
        """

        ret = super(InfoplusHarvester, self).fetch_stage(harvest_object)

        return ret

    # -----------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """
        Imports the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the HarvestObject was imported or not
        """

        # harvest_api_key = model.User.get(context['user']).apikey.encode('utf8')

        ret = super(InfoplusHarvester, self).import_stage(harvest_object)

        return ret
