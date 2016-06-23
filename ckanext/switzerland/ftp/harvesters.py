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
    remotefolder = 'infoplus'

    # subfolder in the above remote folder
    environment = 'test'

    # -----------------------------------------------------------------------

    def gather_stage(self, harvest_job):
        """
        Gathers resources to fetch

        :param harvest_job: Harvester job
        :returns: object_ids list List of HarvestObject ids that are processed in the next stage (fetch_stage)
        """
        log.debug('In %s FTPHarvester gather_stage' % self.harvester_name) # harvest_job.source.url

        ret = super(InfoplusHarvester, self).gather_stage(harvest_job)

        return ret

    # -----------------------------------------------------------------------

    def fetch_stage(self, harvest_object):
        """
        Fetching of resources

        :param harvest_object: HarvestObject
        :returns: True|None Whether HarvestObject was saved or not
        """
        log.debug('In %s FTPHarvester fetch_stage' % self.harvester_name) # harvest_job.source.url

        ret = super(InfoplusHarvester, self).fetch_stage(harvest_job)

        return ret

    # -----------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """
        Importing the fetched files into CKAN storage

        :param harvest_object: HarvestObject
        :returns: True|False boolean Whether the HarvestObject was imported or not
        """
        log.debug('In %s FTPHarvester import_stage' % self.harvester_name) # harvest_job.source.url

        # harvest_api_key = model.User.get(context['user']).apikey.encode('utf8')

        ret = super(InfoplusHarvester, self).gather_stage(harvest_job)

        return ret


# =======================================================================


class DidokHarvester(BaseFTPHarvester):
    """
    An FTP Harvester for Didok data
    """

    # name of the harvester
    harvester_name = 'Didok'

    # parent folder of the data on the ftp server
    remotefolder = 'didok'

    # subfolder in the above remote folder
    environment = 'test'

    # -----------------------------------------------------------------------

    def gather_stage(self, harvest_job):
        """
        Gathers resources to fetch
        """
        log.debug('In %s FTPHarvester gather_stage' % self.harvester_name) # harvest_job.source.url

        ret = super(InfoplusHarvester, self).gather_stage(harvest_job)

        return ret

    # -----------------------------------------------------------------------

    def fetch_stage(self, harvest_object):
        """
        Fetching of resources
        """
        log.debug('In %s FTPHarvester fetch_stage' % self.harvester_name) # harvest_job.source.url

        ret = super(InfoplusHarvester, self).fetch_stage(harvest_job)

        return ret

    # -----------------------------------------------------------------------

    def import_stage(self, harvest_object):
        """
        Imports the fetched files into CKAN storage
        """
        log.debug('In %s FTPHarvester import_stage' % self.harvester_name) # harvest_job.source.url

        # harvest_api_key = model.User.get(context['user']).apikey.encode('utf8')

        ret = super(InfoplusHarvester, self).gather_stage(harvest_job)

        return ret
