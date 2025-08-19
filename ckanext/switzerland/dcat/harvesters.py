import logging

from ckanext.dcat.harvesters.rdf import DCATRDFHarvester

log = logging.getLogger(__name__)


class SwissDCATRDFHarvester(DCATRDFHarvester):
    def info(self):
        return {
            "name": "dcat_ch_rdf",
            "title": "DCAT-AP Switzerland RDF Harvester",
            "description": "Harvester for DCAT-AP Switzerland datasets from an RDF graph",
        }

    def _get_guid(self, dataset_dict, source_url=None):
        """
        Try to get a unique identifier for a harvested dataset
        It will be the first found of:
         * URI (rdf:about)
         * dcat:identifier
         * Source URL + Dataset name
         * Dataset name
         The last two are obviously not optimal, as depend on title, which might change.
         Returns None if no guid could be decided.
        """
        uri = self._get_dict_value(dataset_dict, "uri")
        if uri:
            return uri

        identifier = self._get_dict_value(dataset_dict, "identifier")
        if identifier:
            return identifier

        guid = None
        name = self._get_dict_value(dataset_dict, "name")
        if name:
            guid = name
            if source_url:
                guid = source_url.rstrip("/") + "/" + guid

        return guid

    def _gen_new_name(self, title):
        try:
            return super(SwissDCATRDFHarvester, self)._gen_new_name(title["de"])
        except TypeError:
            return super(SwissDCATRDFHarvester, self)._gen_new_name(title)
