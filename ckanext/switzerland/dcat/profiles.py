import logging
import re
import time
from datetime import datetime

import rdflib
from ckan.lib.helpers import url_for
from rdflib import BNode, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SKOS, Namespace

from ckanext.dcat.profiles import RDFProfile
from ckanext.dcat.utils import resource_uri
from ckanext.switzerland.helpers import (
    get_langs,
    map_to_valid_format,
    ogdch_get_default_terms_of_use,
)

log = logging.getLogger(__name__)


DCT = Namespace("http://purl.org/dc/terms/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
SCHEMA = Namespace("http://schema.org/")
ADMS = Namespace("http://www.w3.org/ns/adms#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
TIME = Namespace("http://www.w3.org/2006/time")
LOCN = Namespace("http://www.w3.org/ns/locn#")
GSP = Namespace("http://www.opengis.net/ont/geosparql#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")
SPDX = Namespace("http://spdx.org/rdf/terms#")
XML = Namespace("http://www.w3.org/2001/XMLSchema")

GEOJSON_IMT = "https://www.iana.org/assignments/media-types/application/vnd.geo+json"

namespaces = {
    "dct": DCT,
    "dcat": DCAT,
    "adms": ADMS,
    "vcard": VCARD,
    "foaf": FOAF,
    "schema": SCHEMA,
    "time": TIME,
    "skos": SKOS,
    "locn": LOCN,
    "gsp": GSP,
    "owl": OWL,
    "xml": XML,
}

slug_id_pattern = re.compile("[^/]+(?=/$|$)")


class SwissDCATAPProfile(RDFProfile):
    """
    An RDF profile for the DCAT-AP Switzerland recommendation for data portals

    It requires the European DCAT-AP profile (`euro_dcat_ap`)
    """

    def _object_value(self, subject, predicate, multilang=False):
        """
        Given a subject and a predicate, returns the value of the object

        Both subject and predicate must be rdflib URIRef or BNode objects

        If found, the unicode representation is returned, else None
        """
        default_lang = "de"
        lang_dict = {}
        for o in self.g.objects(subject, predicate):
            if multilang and o.language:
                lang_dict[o.language] = str(o)
            elif multilang:
                lang_dict[default_lang] = str(o)
            else:
                return str(o)
        if multilang:
            # when translation does not exist, create an empty one
            for lang in get_langs():
                if lang not in lang_dict:
                    lang_dict[lang] = ""
        return lang_dict

    def _publisher(self, subject, predicate):
        """Overwritten from parent method to get name as multilang value."""
        publisher = {}

        for agent in self.g.objects(subject, predicate):
            publisher["uri"] = (
                str(agent) if isinstance(agent, rdflib.term.URIRef) else ""
            )
            publisher["name"] = self._object_value(agent, FOAF.name, multilang=True)
            publisher["email"] = self._object_value(agent, FOAF.mbox)
            publisher["url"] = self._object_value(agent, FOAF.homepage)
            publisher["type"] = self._object_value(agent, DCT.type)

        return publisher

    def _relations(self, subject, predicate):
        relations = []

        for relation_node in self.g.objects(subject, predicate):
            relation = {
                "label": self._object_value(relation_node, RDFS.label),
                "url": relation_node,
            }
            relations.append(relation)

        return relations

    def _keywords(self, subject, predicate):
        keywords = {}

        for lang in get_langs():
            keywords[lang] = []

        for keyword_node in self.g.objects(subject, predicate):
            keywords[keyword_node.language].append(str(keyword_node))

        return keywords

    def _contact_points(self, subject, predicate):
        contact_points = []

        for contact_node in self.g.objects(subject, predicate):
            email = self._object_value(contact_node, VCARD.hasEmail)
            email_clean = email.replace("mailto:", "")
            contact = {
                "name": self._object_value(contact_node, VCARD.fn),
                "email": email_clean,
            }

            contact_points.append(contact)

        return contact_points

    def _temporals(self, subject, predicate):
        temporals = []

        for temporal_node in self.g.objects(subject, predicate):
            start_date = self._object_value(temporal_node, SCHEMA.startDate)
            end_date = self._object_value(temporal_node, SCHEMA.endDate)
            if start_date or end_date:
                temporals.append(
                    {
                        "start_date": self._clean_datetime(start_date),
                        "end_date": self._clean_datetime(end_date),
                    }
                )

        return temporals

    def _clean_datetime(self, datetime_value):
        try:
            d = datetime.strptime(datetime_value[0 : len("YYYY-MM-DD")], "%Y-%m-%d")
            return int(time.mktime(d.timetuple()))
        except (ValueError, KeyError, TypeError, IndexError):
            return None

    def _add_multilang_value(self, subject, predicate, dataset_key, dataset_dict):
        multilang_values = dataset_dict.get(dataset_key)
        if multilang_values:
            for key, values in list(multilang_values.items()):
                if values:
                    # the values can be either a multilang-dict or they are
                    # nested in another iterable (e.g. keywords)
                    if not isinstance(values, list):
                        values = [values]
                    for value in values:
                        self.g.add((subject, predicate, Literal(value, lang=key)))

    def parse_dataset(self, dataset_dict, dataset_ref):
        dataset_dict["temporals"] = []
        dataset_dict["tags"] = []
        dataset_dict["extras"] = []
        dataset_dict["resources"] = []
        dataset_dict["relations"] = []
        dataset_dict["see_alsos"] = []

        # Basic fields
        for key, predicate in (
            ("identifier", DCT.identifier),
            ("accrual_periodicity", DCT.accrualPeriodicity),
            ("spatial_uri", DCT.spatial),
            ("spatial", DCT.spatial),
            ("url", DCAT.landingPage),
        ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict[key] = value

        # Timestamp fields
        self._timestamp_fields(dataset_ref, dataset_dict)

        # Multilingual basic fields
        self._multilingual_fields(dataset_ref, dataset_dict)

        # Tags
        keywords = self._object_value_list(dataset_ref, DCAT.keyword) or []
        for keyword in keywords:
            dataset_dict["tags"].append({"name": keyword})

        # Keywords
        dataset_dict["keywords"] = self._keywords(dataset_ref, DCAT.keyword)

        # Themes
        dcat_theme_urls = self._object_value_list(dataset_ref, DCAT.theme)
        if dcat_theme_urls:
            dataset_dict["groups"] = []
            for dcat_theme_url in dcat_theme_urls:
                search_result = slug_id_pattern.search(dcat_theme_url)
                dcat_theme_slug = search_result.group()
                dataset_dict["groups"].append({"name": dcat_theme_slug})

        #  Languages
        languages = self._object_value_list(dataset_ref, DCT.language)
        if languages:
            dataset_dict["language"] = languages

        # Contact details
        dataset_dict["contact_points"] = self._contact_points(
            dataset_ref, DCAT.contactPoint
        )

        # Publisher
        dataset_dict["publisher"] = self._publisher(dataset_ref, DCT.publisher)

        # Relations
        dataset_dict["relations"] = self._relations(dataset_ref, DCT.relation)

        # Temporal
        dataset_dict["temporals"] = self._temporals(dataset_ref, DCT.temporal)

        # References
        see_alsos = self._object_value_list(dataset_ref, RDFS.seeAlso)
        for see_also in see_alsos:
            dataset_dict["see_alsos"].append({"dataset_identifier": see_also})

        # Dataset URI (explicitly show the missing ones)
        dataset_uri = (
            str(dataset_ref) if isinstance(dataset_ref, rdflib.term.URIRef) else None
        )
        dataset_dict["extras"].append({"key": "uri", "value": dataset_uri})

        # Resources
        for distribution in self._distributions(dataset_ref):
            self._parse_resource(dataset_dict, distribution)

        return dataset_dict

    def _parse_resource(self, dataset_dict, distribution):
        resource_dict = {
            "media_type": None,
            "language": [],
        }
        #  Simple values
        for key, predicate in (
            ("identifier", DCT.identifier),
            ("format", DCT["format"]),
            ("mimetype", DCAT.mediaType),
            ("media_type", DCAT.mediaType),
            ("download_url", DCAT.downloadURL),
            ("url", DCAT.accessURL),
            ("rights", DCT.rights),
            ("license", DCT.license),
        ):
            value = self._object_value(distribution, predicate)
            if value:
                resource_dict[key] = value
        # if media type is not set, use format as fallback
        if not resource_dict.get("media_type") and resource_dict.get("format"):
            resource_dict["media_type"] = resource_dict["mimetype"] = resource_dict[
                "format"
            ]
        # Timestamp fields
        self._timestamp_fields(distribution, resource_dict)
        # Multilingual fields
        self._multilingual_fields(distribution, resource_dict)
        resource_dict["url"] = self._object_value(
            distribution, DCAT.accessURL
        ) or self._object_value(distribution, DCAT.downloadURL)
        # languages
        for language in self._object_value_list(distribution, DCAT.language):
            resource_dict["language"].append(language)
        # byteSize
        byte_size = self._object_value_int(distribution, DCAT.byteSize)
        if byte_size is not None:
            resource_dict["byte_size"] = byte_size
        # Distribution URI (explicitly show the missing ones)
        resource_dict["uri"] = (
            str(distribution) if isinstance(distribution, rdflib.term.URIRef) else None
        )
        dataset_dict["resources"].append(resource_dict)

    def _multilingual_fields(self, subject, destination_dict):
        for key, predicate in (
            ("title", DCT.title),
            ("description", DCT.description),
        ):
            value = self._object_value(subject, predicate, multilang=True)
            if value:
                destination_dict[key] = value

    def _timestamp_fields(self, subject, destination_dict):
        for key, predicate in (
            ("issued", DCT.issued),
            ("modified", DCT.modified),
        ):
            value = self._object_value(subject, predicate)
            if value:
                destination_dict[key] = self._clean_datetime(value)

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        g = self.g

        for prefix, namespace in list(namespaces.items()):
            g.bind(prefix, namespace)

        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        # Basic fields
        items = [
            ("version", OWL.versionInfo, ["dcat_version"], Literal),
            ("version_notes", ADMS.versionNotes, None, Literal),
            ("frequency", DCT.accrualPeriodicity, None, Literal),
            ("access_rights", DCT.accessRights, None, Literal),
            ("dcat_type", DCT.type, None, Literal),
            ("provenance", DCT.provenance, None, Literal),
            ("spatial", DCT.spatial, None, Literal),
        ]

        g.add(
            (
                dataset_ref,
                DCT.identifier,
                Literal(
                    "{}@{}".format(
                        dataset_dict["name"], dataset_dict["organization"]["name"]
                    )
                ),
            )
        )

        self._add_triples_from_dict(dataset_dict, dataset_ref, items)

        self._add_multilang_value(
            dataset_ref, DCT.description, "description", dataset_dict
        )
        self._add_multilang_value(dataset_ref, DCT.title, "title", dataset_dict)

        # LandingPage
        g.add(
            (
                dataset_ref,
                DCAT.landingPage,
                URIRef(
                    url_for(
                        "dataset.read",
                        id=dataset_dict["name"],
                        qualified=True,
                        locale="default",
                    )
                ),
            )
        )
        self._add_multilang_value(dataset_ref, DCAT.keyword, "keywords", dataset_dict)

        # Dates
        items = [
            ("issued", DCT.issued, ["metadata_created"], Literal),
            ("modified", DCT.modified, ["metadata_modified"], Literal),
        ]
        self._add_date_triples_from_dict(dataset_dict, dataset_ref, items)

        # Update Interval
        accrual_periodicity = dataset_dict.get("accrual_periodicity")
        if accrual_periodicity:
            g.add((dataset_ref, DCT.accrualPeriodicity, URIRef(accrual_periodicity)))

        # Lists
        items = [
            ("language", DCT.language, None, Literal),
            ("conforms_to", DCT.conformsTo, None, Literal),
            ("alternate_identifier", ADMS.identifier, None, Literal),
            ("documentation", FOAF.page, None, Literal),
            ("has_version", DCT.hasVersion, None, Literal),
            ("is_version_of", DCT.isVersionOf, None, Literal),
            ("source", DCT.source, None, Literal),
            ("sample", ADMS.sample, None, Literal),
        ]
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, items)

        # Relations
        if dataset_dict.get("relations"):
            relations = dataset_dict.get("relations")
            for relation in relations:
                relation_name = relation["label"]
                relation_url = relation["url"]

                relation = URIRef(relation_url)
                g.add((relation, RDFS.label, Literal(relation_name)))
                g.add((dataset_ref, DCT.relation, relation))

        # Add the dataset's terms of use as a relation
        terms_of_use_name = (
            dataset_dict["odpch_license_name"]
            if dataset_dict.get("odpch_license_name")
            else ogdch_get_default_terms_of_use()["name"]
        )
        terms_of_use_url = (
            dataset_dict["odpch_license_url"]
            if dataset_dict.get("odpch_license_url")
            else ogdch_get_default_terms_of_use()["url"]
        )

        relation = URIRef(terms_of_use_url)
        g.add((relation, RDFS.label, Literal(terms_of_use_name)))
        g.add((dataset_ref, DCT.relation, relation))

        # References
        if dataset_dict.get("see_alsos"):
            references = dataset_dict.get("see_alsos")
            for reference in references:
                reference_identifier = reference["dataset_identifier"]
                g.add((dataset_ref, RDFS.seeAlso, Literal(reference_identifier)))

        # Contact details
        if dataset_dict.get("contact_points"):
            contact_points = self._get_dataset_value(dataset_dict, "contact_points")
            for contact_point in contact_points:
                contact_details = BNode()
                contact_point_email = contact_point["email"]
                contact_point_name = contact_point["name"]

                g.add((contact_details, RDF.type, VCARD.Organization))
                g.add((contact_details, VCARD.hasEmail, URIRef(contact_point_email)))
                g.add((contact_details, VCARD.fn, Literal(contact_point_name)))

                g.add((dataset_ref, DCAT.contactPoint, contact_details))

        # Publisher
        self._add_publisher_to_graph(dataset_dict, dataset_ref)

        # Temporals
        self._add_temporals_to_graph(dataset_dict, dataset_ref)

        # Themes
        g.add(
            (
                dataset_ref,
                DCAT.theme,
                URIRef(
                    "http://publications.europa.eu/resource/authority/data-theme/TRAN"
                ),
            )
        )

        # Resources
        for resource_dict in dataset_dict.get("resources", []):
            self._add_distribution_to_graph(dataset_ref, resource_dict)

    def _add_publisher_to_graph(self, dataset_dict, dataset_ref):
        publisher = dataset_dict.get("publisher")
        if publisher:
            publisher_details = URIRef(publisher["url"])
            self.g.add((publisher_details, RDF.type, FOAF.Agent))
            self._add_multilang_value(publisher_details, FOAF.name, "name", publisher)
            self.g.add((dataset_ref, DCT.publisher, publisher_details))

    def _add_temporals_to_graph(self, dataset_dict, dataset_ref):
        temporals = dataset_dict.get("temporals")
        if temporals:
            for temporal in temporals:
                start = temporal["start_date"]
                end = temporal["end_date"]
                if start or end:
                    temporal_extent = BNode()
                    self.g.add((temporal_extent, RDF.type, DCT.PeriodOfTime))
                    if start:
                        self._add_date_triple(temporal_extent, SCHEMA.startDate, start)
                    if end:
                        self._add_date_triple(temporal_extent, SCHEMA.endDate, end)
                    self.g.add((dataset_ref, DCT.temporal, temporal_extent))

    def _add_distribution_to_graph(self, dataset_ref, resource_dict):
        g = self.g

        distribution = URIRef(resource_uri(resource_dict))
        g.add((dataset_ref, DCAT.distribution, distribution))
        g.add((distribution, RDF.type, DCAT.Distribution))
        #  Simple values
        items = [
            ("status", ADMS.status, None, Literal),
            ("identifier", DCT.identifier, None, Literal),
            ("media_type", DCAT.mediaType, ["mimetype"], Literal),
            ("spatial", DCT.spatial, None, Literal),
            ("license", DCT.license, None, URIRef),
        ]
        self._add_triples_from_dict(resource_dict, distribution, items)
        self._add_multilang_value(distribution, DCT.title, "title", resource_dict)
        self._add_multilang_value(
            distribution, DCT.description, "description", resource_dict
        )
        #  Lists
        items = [
            ("documentation", FOAF.page, None, Literal),
            ("language", DCT.language, None, Literal),
            ("conforms_to", DCT.conformsTo, None, Literal),
        ]
        self._add_list_triples_from_dict(resource_dict, distribution, items)
        # URL
        url = resource_dict.get("url")
        g.add((distribution, DCAT.accessURL, URIRef(url)))
        if resource_dict["url_type"] == "upload":
            g.add((distribution, DCAT.downloadURL, URIRef(url)))

            # Format from Download-Url
            format_value = str(url).rsplit(".", 1)[1]
            mapped_format = map_to_valid_format(format_value)
            g.add((distribution, DCT["format"], Literal(mapped_format)))
        # Mime-Type
        if resource_dict.get("mimetype"):
            g.add((distribution, DCAT.mediaType, Literal(resource_dict["mimetype"])))
        # Dates
        items = [
            ("issued", DCT.issued, None, Literal),
            ("modified", DCT.modified, None, Literal),
        ]
        self._add_date_triples_from_dict(resource_dict, distribution, items)
        # Numbers
        if resource_dict.get("byte_size"):
            g.add((distribution, DCAT.byteSize, Literal(resource_dict["byte_size"])))

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        g = self.g
        g.add((catalog_ref, RDF.type, DCAT.Catalog))


class SwissSchemaOrgProfile(SchemaOrgProfile, MultiLangProfile):
    def _basic_fields_graph(self, dataset_ref, dataset_dict):
        items = [
            ("identifier", SCHEMA.identifier, None, Literal),
            ("version", SCHEMA.version, ["dcat_version"], Literal),
            ("issued", SCHEMA.datePublished, None, Literal),
            ("modified", SCHEMA.dateModified, None, Literal),
            ("author", SCHEMA.author, ["contact_name", "maintainer"], Literal),
            ("url", SCHEMA.sameAs, None, Literal),
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items)

        items = [
            ("title", SCHEMA.name, None, Literal),
            ("description", SCHEMA.description, None, Literal),
        ]
        self._add_multilang_triples_from_dict(dataset_dict, dataset_ref, items)

    def _publisher_graph(self, dataset_ref, dataset_dict):
        if any(
                [
                    self._get_dataset_value(dataset_dict, "publisher_uri"),
                    self._get_dataset_value(dataset_dict, "publisher_name"),
                    dataset_dict.get("organization"),
                ]
        ):
            publisher_uri, publisher_name = dh.get_publisher_dict_from_dataset(
                dataset_dict.get("publisher")
            )
            if publisher_uri:
                publisher_details = CleanedURIRef(publisher_uri)
            else:
                publisher_details = BNode()

            self.g.add((publisher_details, RDF.type, SCHEMA.Organization))
            self.g.add((dataset_ref, SCHEMA.publisher, publisher_details))
            self.g.add((dataset_ref, SCHEMA.sourceOrganization, publisher_details))

            if not publisher_name and dataset_dict.get("organization"):
                publisher_name = dataset_dict["organization"]["title"]
                self._add_multilang_value(
                    publisher_details, SCHEMA.name, multilang_values=publisher_name
                )
            else:
                self.g.add((publisher_details, SCHEMA.name, Literal(publisher_name)))

            contact_point = BNode()
            self.g.add((publisher_details, SCHEMA.contactPoint, contact_point))

            self.g.add((contact_point, SCHEMA.contactType, Literal("customer service")))

            publisher_url = self._get_dataset_value(dataset_dict, "publisher_url")
            if not publisher_url and dataset_dict.get("organization"):
                publisher_url = dataset_dict["organization"].get("url") or config.get(
                    "ckan.site_url", ""
                )

            self.g.add((contact_point, SCHEMA.url, Literal(publisher_url)))
            items = [
                (
                    "publisher_email",
                    SCHEMA.email,
                    ["contact_email", "maintainer_email", "author_email"],
                    Literal,
                ),
                (
                    "publisher_name",
                    SCHEMA.name,
                    ["contact_name", "maintainer", "author"],
                    Literal,
                ),
            ]

            self._add_triples_from_dict(dataset_dict, contact_point, items)

    def _temporal_graph(self, dataset_ref, dataset_dict):
        # schema.org temporalCoverage only allows to specify one temporal
        # DCAT-AP Switzerland allows to specify multiple
        # for the mapping we always use the first one
        temporals = self._get_dataset_value(dataset_dict, "temporals")
        try:
            start = temporals[0].get("start_date")
            end = temporals[0].get("end_date")
        except (IndexError, KeyError, TypeError):
            # do not add temporals if there are none
            return
        if start or end:
            if start and end:
                self.g.add(
                    (
                        dataset_ref,
                        SCHEMA.temporalCoverage,
                        Literal(f"{start}/{end}"),
                    )
                )
            elif start:
                self._add_date_triple(dataset_ref, SCHEMA.temporalCoverage, start)
            elif end:
                self._add_date_triple(dataset_ref, SCHEMA.temporalCoverage, end)

    def _tags_graph(self, dataset_ref, dataset_dict):
        for tag in dataset_dict.get("keywords", []):
            items = [
                ("keywords", SCHEMA.keywords, None, Literal),
            ]
            self._add_multilang_triples_from_dict(dataset_dict, dataset_ref, items)

    def _distribution_basic_fields_graph(self, distribution, resource_dict):
        items = [
            ("issued", SCHEMA.datePublished, None, Literal),
            ("modified", SCHEMA.dateModified, None, Literal),
        ]

        self._add_triples_from_dict(resource_dict, distribution, items)

        items = [
            ("title", SCHEMA.name, None, Literal),
            ("description", SCHEMA.description, None, Literal),
        ]
        self._add_multilang_triples_from_dict(resource_dict, distribution, items)

    def contact_details(self, dataset_dict, dataset_ref, g):
        # Contact details used by graph_from_dataset
        if dataset_dict.get("contact_points"):
            contact_points = self._get_dataset_value(dataset_dict, "contact_points")
            for contact_point in contact_points:
                if not contact_point.get("email") or not contact_point.get("name"):
                    continue
                contact_details = BNode()
                contact_point_email = EMAIL_MAILTO_PREFIX + contact_point["email"]
                contact_point_name = contact_point["name"]

                g.add((contact_details, RDF.type, VCARD.Organization))
                g.add((contact_details, VCARD.hasEmail, URIRef(contact_point_email)))
                g.add((contact_details, VCARD.fn, Literal(contact_point_name)))

                g.add((dataset_ref, SCHEMA.contactPoint, contact_details))

        return g

    def download_access_url(self, resource_dict, distribution, g):
        # Download URL & Access URL used by graph_from_dataset
        download_url = resource_dict.get("download_url")
        if download_url:
            try:
                download_url = dh.uri_to_iri(download_url)
                g.add((distribution, SCHEMA.downloadURL, URIRef(download_url)))
            except ValueError:
                # only add valid URL
                pass

        url = resource_dict.get("url")
        if (url and not download_url) or (url and url != download_url):
            try:
                url = dh.uri_to_iri(url)
                g.add((distribution, SCHEMA.accessURL, URIRef(url)))
            except ValueError:
                # only add valid URL
                pass
        elif download_url:
            g.add((distribution, SCHEMA.accessURL, URIRef(download_url)))

        return g

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        dataset_uri = dh.dataset_uri(dataset_dict, dataset_ref)
        dataset_ref = URIRef(dataset_uri)
        g = self.g

        # Contact details
        self.contact_details(dataset_dict, dataset_ref, g)

        # Resources
        for resource_dict in dataset_dict.get("resources", []):
            distribution = URIRef(dh.resource_uri(resource_dict))

            g.add((dataset_ref, SCHEMA.distribution, distribution))
            g.add((distribution, RDF.type, SCHEMA.Distribution))

            #  Simple values
            items = [
                ("status", ADMS.status, None, Literal),
                ("coverage", DCT.coverage, None, Literal),
                ("identifier", DCT.identifier, None, Literal),
                ("spatial", DCT.spatial, None, Literal),
            ]

            self._add_triples_from_dict(resource_dict, distribution, items)

            self._add_multilang_value(
                distribution, DCT.title, "display_name", resource_dict
            )
            self._add_multilang_value(
                distribution, DCT.description, "description", resource_dict
            )

            # Language
            languages = resource_dict.get("language", [])
            for lang in languages:
                if "http://publications.europa.eu/resource/authority" in lang:
                    # Already a valid EU language URI
                    g.add((distribution, DCT.language, URIRef(lang)))
                else:
                    uri = language_uri_map.get(lang, None)
                    if uri:
                        g.add((distribution, DCT.language, URIRef(uri)))
                    else:
                        log.debug(f"Language '{lang}' not found in language_uri_map")

            # Download URL & Access URL
            self.download_access_url(resource_dict, distribution, g)

            # Dates
            items = [
                ("issued", DCT.issued, None, Literal),
                ("modified", DCT.modified, None, Literal),
            ]

            self._add_date_triples_from_dict(resource_dict, distribution, items)
            # ByteSize
            if resource_dict.get("byte_size"):
                g.add(
                    (distribution, SCHEMA.byteSize, Literal(resource_dict["byte_size"]))
                )

        super(SwissSchemaOrgProfile, self).graph_from_dataset(dataset_dict, dataset_ref)

    def parse_dataset(self, dataset_dict, dataset_ref):
        super(SwissSchemaOrgProfile, self).parse_dataset(dataset_dict, dataset_ref)

