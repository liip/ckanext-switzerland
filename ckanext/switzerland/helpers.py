import ast
import json
import logging
import os
import unicodedata
from collections import OrderedDict, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import ckan.plugins.toolkit as tk
import requests
from ckan.common import _
from ckan.lib.helpers import _link_to
from ckan.lib.helpers import dataset_display_name as dataset_display_name_orig
from ckan.lib.helpers import organization_link as organization_link_orig
from ckan.lib.helpers import url_for
from ckan.lib.munge import munge_filename, munge_title_to_name

log = logging.getLogger(__name__)

DATETIME_FIELDS = [
    "created",
    "last_modified",
    "metadata_created",
    "metadata_modified",
    "issued",
    "modified",
    "version",
]
UTC = ZoneInfo("UTC")
ZURICH = ZoneInfo("Europe/Zurich")
TERMS_OF_USE_OPEN = "http://dcat-ap.ch/vocabulary/licenses/terms_open"
TERMS_OF_USE_BY = "http://dcat-ap.ch/vocabulary/licenses/terms_by"
TERMS_OF_USE_ASK = "http://dcat-ap.ch/vocabulary/licenses/terms_ask"
TERMS_OF_USE_BY_ASK = "http://dcat-ap.ch/vocabulary/licenses/terms_by_ask"


def get_langs():
    language_priorities = ["en", "de", "fr", "it"]
    return language_priorities


def get_localized_value(lang_dict, desired_lang_code=None, default_value=""):
    # return original value if it's not a dict
    if not isinstance(lang_dict, dict):
        return lang_dict

    """
    if this is not a proper lang_dict ('de', 'fr', etc. keys),
    return original value
    """
    if not all(k in lang_dict for k in get_langs()):
        return lang_dict

    # if no specific lang is requested, read from environment
    if desired_lang_code is None:
        desired_lang_code = get_request_language()

    try:
        # return desired lang if available
        if lang_dict[desired_lang_code]:
            return lang_dict[desired_lang_code]
    except KeyError:
        pass

    return _lang_fallback(lang_dict, default_value)


def parse_and_localize(string_lang_dict):
    lang_dict = parse_json(string_lang_dict)
    return get_localized_value(lang_dict)


def _lang_fallback(lang_dict, default_value):
    # loop over languages in order of their priority for fallback
    for lang_code in get_langs():
        try:
            if isinstance(lang_dict[lang_code], str) and lang_dict[lang_code]:
                return lang_dict[lang_code]
        except (KeyError, IndexError, ValueError):
            continue
    return default_value


def ogdch_get_media_type_choices(field):
    return [
        {"label": "application/gzip", "value": "application/gzip"},
        {"label": "application/json", "value": "application/json"},
        {"label": "application/protobuf", "value": "application/protobuf"},
        {
            "label": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "value": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        },
        {"label": "application/xml", "value": "application/xml"},
        {"label": "application/zip", "value": "application/zip"},
        {"label": "text/csv", "value": "text/csv"},
    ]


def get_default_licence_for_organization(org_dict):
    if org_dict["name"] == "astra":
        return TERMS_OF_USE_BY_ASK
    return TERMS_OF_USE_BY


def ogdch_get_accrual_periodicity_choices(field):
    map = [
        {"label": label, "value": value}
        for value, label in get_frequency_name(get_map=True).items()
    ]
    return map


def ogdch_get_license_choices(field):
    return [
        {
            "label": _(
                "Non-commercial Allowed / Commercial Allowed / Reference Not Required"
            ),
            "value": TERMS_OF_USE_OPEN,
        },
        {
            "label": _(
                "Non-commercial Allowed / Commercial With Permission Allowed / Reference Not Required"
            ),
            "value": TERMS_OF_USE_ASK,
        },
        {
            "label": _(
                "Non-commercial Allowed / Commercial With Permission Allowed / Reference Required"
            ),
            "value": TERMS_OF_USE_BY_ASK,
        },
        {
            "label": _(
                "Non-commercial Allowed / Commercial Allowed / Reference Required"
            ),
            "value": TERMS_OF_USE_BY,
        },
    ]


def get_frequency_name(identifier=None, get_map=False):
    frequencies = OrderedDict(
        [
            (
                "http://publications.europa.eu/resource/authority/frequency/IRREG",
                _("Irregular"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/CONT",
                _("Continuous"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/HOURLY",
                _("Hourly"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/DAILY",
                _("Daily"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/WEEKLY_3",
                _("Three times a week"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/WEEKLY_2",
                _("Two times a week"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/WEEKLY",
                _("Weekly"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/MONTHLY_3",
                _("Three times a month"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY",
                _("Every two weeks"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/MONTHLY_2",
                _("Twice a month"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/MONTHLY",
                _("Monthly"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/BIMONTHLY",
                _("Every two months"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/QUARTERLY",
                _("Every three months"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/ANNUAL_3",
                _("Three times a year"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2",
                _("Twice a year"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/ANNUAL",
                _("Annual"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/BIENNIAL",
                _("Every two years"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/TRIENNIAL",
                _("Every three years"),
            ),
            (
                "http://publications.europa.eu/resource/authority/frequency/OTHER",
                _("Other"),
            ),
        ]
    )
    if get_map:
        return frequencies
    try:
        return frequencies[identifier]
    except KeyError:
        return identifier


def simplify_terms_of_use(term_id):
    terms = [
        "NonCommercialAllowed-CommercialAllowed-ReferenceNotRequired",
        "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
        "NonCommercialAllowed-CommercialWithPermission-ReferenceNotRequired",
        "NonCommercialAllowed-CommercialWithPermission-ReferenceRequired",
    ]

    if term_id in terms:
        return term_id
    return "ClosedData"


def get_readable_file_size(num, suffix="B"):
    try:
        for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
            num = float(num)
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, "Y", suffix)
    except (ValueError, TypeError):
        return False


def parse_json(value, default_value=None):
    # when the value is a string integer like "123" we do not want this to be converted
    # to a real integer py json.loads
    try:
        int(value)
        return value
    except (ValueError, TypeError):
        pass
    try:
        return json.loads(value)
    except (ValueError, TypeError, AttributeError):
        if default_value is not None:
            return default_value
        return value


def get_content_headers(url):
    response = requests.head(url)
    return response


def convert_post_data_to_dict(field_name, data):
    d = defaultdict(lambda: {})
    for json_field_name, value in list(data.items()):
        if json_field_name.startswith(field_name + "-"):
            counter, json_field_name = json_field_name.split("-")[1:]
            d[counter][json_field_name] = value
    return list(d.values())


# monkey patched version of ckan.lib.helpers.dataset_display_name which extracts the
# correct translation of the dataset
def dataset_display_name(package_or_package_dict):
    name = dataset_display_name_orig(package_or_package_dict)
    name = parse_json(name)
    if isinstance(name, dict):
        name = get_localized_value(name)
    return name


# monkey patched version of ckan.lib.helpers.resource_display_name which extracts the
# correct translation of the dataset
def resource_display_name(resource_dict):
    name = resource_dict.get("name", None)
    description = resource_dict.get("description", None)
    if name:
        name = parse_json(name)
        if isinstance(name, dict):
            name = get_localized_value(name)
        return name
    elif description:
        description = parse_json(description)
        if isinstance(description, dict):
            description = get_localized_value(description)
        if isinstance(description, str):
            description = description.split(".")[0]
            max_len = 60
            if len(description) > max_len:
                description = description[:max_len] + "..."
        return description
    else:
        return _("Unnamed resource")


# monkey patched version of ckan.lib.helpers.organization_link which extracts the
# correct translation of the org
def organization_link(organization):
    organization["title"] = parse_and_localize(organization["title"])
    return organization_link_orig(organization)


# monkey patched version of ckan.lib.helpers.group_link which extracts the correct
# translation of the dataset
def group_link(group):
    url = url_for("group.read", id=group["name"])
    title = group["title"]
    title = parse_json(title)
    # the group creation message contains str(dict), so we must parse the string to fix
    # it
    if isinstance(title, str):
        title = ast.literal_eval(title)
    if isinstance(title, dict):
        title = get_localized_value(title)
    return _link_to(title, url)


# patch activity
def resource_link(resource_dict, package_id):
    # ---
    # issue: resource_dict['name'] is saved as str(dict), and therefore is invalid json
    #   -> parse_json just returns the string
    # resolutions: parse the invalid json string into a dict
    # ---
    if "name" in resource_dict and resource_dict["name"]:
        resource_dict["name"] = get_localized_value(
            ast.literal_eval(resource_dict["name"])
        )

    text = resource_display_name(resource_dict)
    url = url_for(
        "package.resource_read", id=package_id, resource_id=resource_dict["id"]
    )
    return _link_to(text, url)


def resource_filename(resource_url):
    return munge_filename(os.path.basename(resource_url))


# all formats that need to be mapped have to be entered lower-case
def map_to_valid_format(resource_format):
    format_mapping = {
        "CSV": ["csv", "aspx", "text (.csv)", "comma ..."],
        "GeoJSON": ["geojson"],
        "GeoTIFF": ["geotiff"],
        "GPKG": ["gpkg"],
        "HTML": ["html"],
        "INTERLIS": ["interlis"],
        "JSON": ["json"],
        "KMZ": ["kmz"],
        "MULTIFORMAT": ["multiformat"],
        "ODS": ["ods", "vnd.oas..."],
        "PC-AXIS": ["pc-axis file"],
        "PDF": ["pdf"],
        "PNG": ["png"],
        "RDF": ["sparql-..."],
        "SHAPEFILE": [
            "esri shapefile",
            "esri geodatabase (....",
            "esri file geodatabase",
            "esri arcinfo ascii ...",
        ],
        "TXT": ["text", "txt", "text (.txt)", "plain"],
        "TIFF": ["tiff"],
        "WCS": ["wcs"],
        "WFS": ["wfs"],
        "WMS": ["wms"],
        "WMTS": ["wmts"],
        "XLS": ["xls", "xlsx"],
        "XML": ["xml"],
        "ZIP": ["zip"],
    }
    resource_format_lower = resource_format.lower()
    for key, values in list(format_mapping.items()):
        if resource_format_lower in values:
            return key
    else:
        return None


def localize_change_dict(changes):
    """Localize titles and descriptions in a list of changes to a package,
    group or organization.
    """
    multilingual_fields = [
        "title",
        "old_title",
        "new_title",
        "old_org_title",
        "new_org_title",
        "old_desc",
        "new_desc",
        "resource_name",
        "old_resource_name",
        "new_resource_name",
        "old_name",
        "new_name",
    ]
    for change in changes:
        for field in multilingual_fields:
            if field in change:
                change[field] = parse_and_localize(change.get(field, ""))

    return changes


def get_cookie_law_url():
    return tk.config.get("ckanext.switzerland.cookie_law_url", "")


def get_cookie_law_id():
    return tk.config.get("ckanext.switzerland.cookie_law_id", "")


def _get_datetime_from_isoformat_string(dt_string):
    """Get a datetime object from an isoformat string, and handle empty values
    and malformatted strings.

    Returns the datetime object if possible, False if it couldn't get one.
    """
    if dt_string in [None, ""]:
        return False
    try:
        return datetime.fromisoformat(dt_string)
    except ValueError:
        log.warning(f"Error getting a datetime from isoformat string: {dt_string}")
        return False


def convert_datetimes_for_api(dataset_or_resource_dict):
    """Calculates the time of a datetime in the Europe/Zurich time zone and outputs the
    value as isoformat, with time zone info.

    All datetimes are stored in the database and Solr as UTC and have no time zone info.
    """
    for field in DATETIME_FIELDS:
        dt = _get_datetime_from_isoformat_string(dataset_or_resource_dict.get(field))
        if dt is False:
            continue

        dt_utc = dt.replace(tzinfo=UTC)
        dt_zh = dt_utc.astimezone(ZURICH)
        dataset_or_resource_dict[field] = dt_zh.isoformat()

    if dataset_or_resource_dict.get("resources"):
        for resource in dataset_or_resource_dict["resources"]:
            convert_datetimes_for_api(resource)


def request_is_api_request():
    try:
        path = tk.request.path
        if any(
            [
                path.endswith(".xml"),
                path.endswith(".rdf"),
                path.endswith(".n3"),
                path.endswith(".ttl"),
                path.endswith(".jsonld"),
            ]
        ):
            return True
        if path.startswith("/api") and not path.startswith("/api/action"):
            # The API client for CKAN's JS modules uses a path starting
            # /api/action, i.e. without a version number. All other API calls
            # should include a version number.
            return True
    except RuntimeError:
        # we get here if there is no request (i.e. on the command line)
        return False


def clean_up_list_fields(search_data, validated_dict):
    """Remove extra fields that are lists, or lists of dicts.
    This is necessary as of this update to CKAN:
    https://github.com/ckan/ckan/commit/e1dde691fd12283209ccea39592c31e7013b25be
    The package to be updated is found using package_show and validated against
    our schema, so all the extra fields are added to the package dict. We don't
    need them in the Solr document and they will cause an atomic error if left in.
    """
    for key in [
        "publishers",
        "contact_points",
        "relations",
        "temporals",
        "keywords",
        "language",
        "display_name",
    ]:
        if key in search_data:
            if key not in validated_dict:
                # This is a hack. :( Fields that use the validator
                # json_list_of_dicts_field are removed from the dataset dict during
                # validation and I can't work out why. If this has happened, add
                # them back now.
                validated_dict[key] = search_data[key]
            del search_data[key]
    search_data["validated_data_dict"] = json.dumps(validated_dict)

    res_description = search_data.get("res_description", [])
    if len(res_description) > 0 and not isinstance(res_description[0], str):
        # res_description should be a list of strings (the multilingual dict
        # of each resource description, dumped to a string). If the package dict was
        # found using package_show, it will be a list of dicts.
        search_data["res_description"] = [
            json.dumps(description)
            for description in search_data.get("res_description", [])
        ]


def index_language_specific_values(search_data, validated_dict):
    text_field_items = {}
    for lang_code in get_langs():
        validated_title = validated_dict.get("title", "")
        search_data["title_" + lang_code] = get_localized_value(
            validated_title, lang_code
        )
        search_data["title_string_" + lang_code] = munge_title_to_name(
            get_localized_value(validated_title, lang_code)
        )

        validated_description = validated_dict.get("description", "")
        search_data["description_" + lang_code] = get_localized_value(
            validated_description, lang_code
        )
        text_field_items["text_" + lang_code] = [
            get_localized_value(validated_description, lang_code)
        ]

        validated_keywords = validated_dict.get("keywords", [])
        search_data["keywords_" + lang_code] = validated_keywords.get(lang_code, [])
        if search_data["keywords_" + lang_code]:
            text_field_items["text_" + lang_code].append(
                " ".join(search_data["keywords_" + lang_code])
            )

    # flatten values for text_* fields
    for key, value in list(text_field_items.items()):
        search_data[key] = " ".join(value)


def get_request_language():
    try:
        return tk.request.environ["CKAN_LANG"]
    except (KeyError, RuntimeError, TypeError):
        return tk.config.get("ckan.locale_default", "en")


def get_wordpress_url():
    return tk.config.get("ckanext.switzerland.wp_url")


def strxfrm(s):
    """Overriden from ckan.lib.helpers.strxfrm to handle our multilingual fields."""
    s = parse_and_localize(s)
    return unicodedata.normalize("NFD", s).lower()


def ogdch_render_publisher(publisher_value):
    """Return the publisher in the format
    {
        "name": {
            "de": "German Name",
            "en": "English Name",
            "fr": "French Name",
            "it": "Italian Name"
            },
        "url": "Publisher URL"
        }

    The input might be
    - a dict
    - a dict serialised to a json string
    - a list of dicts, [{"label": "Publisher Name"}, ...] (deprecated September 2025)
    """
    if isinstance(publisher_value, list):
        return {
            "name": {lang: publisher_value[0]["label"] for lang in get_langs()},
            "url": "",
        }

    try:
        publisher = json.loads(publisher_value)
    except TypeError:
        return publisher_value
    else:
        return publisher


def ogdch_publisher_form_helper(data):
    """Fill the publisher form snippet either from a previous form entry or from the db"""

    # check for form inputs first
    publisher_form_name = {
        "fr": data.get("publisher-name-fr", ""),
        "en": data.get("publisher-name-en", ""),
        "de": data.get("publisher-name-de", ""),
        "it": data.get("publisher-name-it", ""),
    }
    publisher_form_url = data.get("publisher-url")

    if publisher_form_url or any(publisher_form_name.values()):
        return {"name": publisher_form_name, "url": publisher_form_url}

    # check for publisher from db
    publisher_stored = data.get("publisher")
    if isinstance(publisher_stored, str):
        try:
            publisher_stored = json.loads(publisher_stored)
        except json.JSONDecodeError:
            log.warning(f"Invalid JSON in publisher: {publisher_stored}")

    if isinstance(publisher_stored, dict):
        publisher_name = publisher_stored.get("name")
        # handle stored publisher data (both as dict or string)
        if isinstance(publisher_name, dict):
            return {"name": publisher_name, "url": publisher_stored.get("url", "")}
        elif isinstance(publisher_name, str):
            return {
                "name": {"de": publisher_name, "en": "", "fr": "", "it": ""},
                "url": publisher_stored.get("url", ""),
            }
        else:
            log.warning("Unexpected structure for publisher name")
            return {
                "name": {"de": "", "en": "", "fr": "", "it": ""},
                "url": publisher_stored.get("url", ""),
            }

    # handle publisher in deprecated format
    publisher_deprecated = _convert_from_publisher_deprecated(data)
    if publisher_deprecated:
        return publisher_deprecated

    return {"name": {"de": "", "en": "", "fr": "", "it": ""}, "url": ""}


def _convert_from_publisher_deprecated(data):
    publishers = data.get("publishers", [])
    if publishers:
        return {
            "name": {lang: publishers[0]["label"] for lang in get_langs()},
            "url": "",
        }

    return None


def ogdch_get_default_terms_of_use():
    return {
        "name": _("Terms of use opentransportdata.swiss"),
        "url": f"https://opentransportdata.swiss/{ _('en/terms-of-use') }",
    }
