import json
import logging
import os
import sys

import ckan.lib.helpers as h
import ckan.lib.uploader as uploader
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import ckanext.switzerland.helpers as sh
from ckanext.switzerland import logic as l
from ckanext.switzerland import validators as v
from ckanext.switzerland.blueprints import ogdch_dataset, ogdch_home

log = logging.getLogger(__name__)


class OgdchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IValidators)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IBlueprint, inherit=True)
    plugins.implements(plugins.IFacets)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_resource("assets", "switzerland")
        toolkit.add_public_directory(config_, "public")

    # IValidators

    def get_validators(self):
        return {
            "multiple_text": v.multiple_text,
            "multiple_text_output": v.multiple_text_output,
            "multilingual_text_output": v.multilingual_text_output,
            "list_of_dicts": v.list_of_dicts,
            "timestamp_to_datetime": v.timestamp_to_datetime,
            "ogdch_multiple_choice": v.ogdch_multiple_choice,
            "ogdch_unique_identifier": v.ogdch_unique_identifier,
            "ogdch_fluent_tags": v.ogdch_fluent_tags,
            "temporals_to_datetime_output": v.temporals_to_datetime_output,
            "json_list_of_dicts_field": v.json_list_of_dicts_field,
            "parse_json": sh.parse_json,
            "url": v.url_validator,
            "name_validator": v.ogdch_name_validator,
            "ogdch_validate_formfield_publisher": v.ogdch_validate_formfield_publisher,
            "ogdch_isodatetime": v.ogdch_isodatetime,
        }

    # IActions

    def get_actions(self):
        """
        Expose new API methods
        """
        return {
            "ogdch_dataset_by_identifier": l.ogdch_dataset_by_identifier,
        }

    # ITemplateHelpers

    def get_helpers(self):
        """
        Provide template helper functions
        """
        return {
            "get_localized_value": sh.get_localized_value,
            "parse_and_localize": sh.parse_and_localize,
            "get_frequency_name": sh.get_frequency_name,
            "get_readable_file_size": sh.get_readable_file_size,
            "parse_json": sh.parse_json,
            "convert_post_data_to_dict": sh.convert_post_data_to_dict,
            "resource_filename": sh.resource_filename,
            "convert_datetimes_for_api": sh.convert_datetimes_for_api,
            "request_is_api_request": sh.request_is_api_request,
            # monkey patch template helpers to return translated names/titles
            "dataset_display_name": sh.dataset_display_name,
            "resource_display_name": sh.resource_display_name,
            "group_link": sh.group_link,
            "resource_link": sh.resource_link,
            "organization_link": sh.organization_link,
            "strxfrm": sh.strxfrm,
            # end monkey-patched helpers
            "get_langs": sh.get_langs,
            "localize_change_dict": sh.localize_change_dict,
            "get_cookie_law_url": sh.get_cookie_law_url,
            "get_cookie_law_id": sh.get_cookie_law_id,
            "get_wordpress_url": sh.get_wordpress_url,
            "ogdch_get_accrual_periodicity_choices": sh.ogdch_get_accrual_periodicity_choices,
            "ogdch_get_license_choices": sh.ogdch_get_license_choices,
            "ogdch_render_publisher": sh.ogdch_render_publisher,
            "ogdch_publisher_form_helper": sh.ogdch_publisher_form_helper,
            "ogdch_get_media_type_choices": sh.ogdch_get_media_type_choices,
            "ogdch_get_default_terms_of_use": sh.ogdch_get_default_terms_of_use,
        }

    def i18n_directory(self):
        # assume plugin is called ckanext.<myplugin>.<...>.PluginClass
        extension_module_name = ".".join(self.__module__.split(".")[:3])
        module = sys.modules[extension_module_name]
        return os.path.abspath(
            os.path.join(os.path.dirname(module.__file__), "../../i18n")
        )

    def i18n_locales(self):
        directory = self.i18n_directory()
        return [
            d
            for d in os.listdir(directory)
            if os.path.isdir(os.path.join(directory, d))
        ]

    def i18n_domain(self):
        return "ckanext-switzerland"

    # IBlueprint

    def get_blueprint(self):
        return [ogdch_dataset, ogdch_home]

    # IFacets

    def _update_facets(self, facets_dict):
        """Remove the Tags and Licenses facet (which we don't use) and add a Keywords facet in the
        language of the current request.
        """
        lang_code = sh.get_request_language()
        facets_dict["keywords_" + lang_code] = toolkit._("Keywords")
        del facets_dict["tags"]
        del facets_dict["license_id"]

        return facets_dict

    def dataset_facets(self, facets_dict, package_type):
        return self._update_facets(facets_dict)

    def group_facets(self, facets_dict, group_type, package_type):
        return self._update_facets(facets_dict)

    def organization_facets(self, facets_dict, organization_type, package_type):
        return self._update_facets(facets_dict)


# monkey patch template helpers to return translated names/titles
h.dataset_display_name = sh.dataset_display_name
h.resource_display_name = sh.resource_display_name
h.group_link = sh.group_link
h.resource_link = sh.resource_link
h.organization_link = sh.organization_link
h.strxfrm = sh.strxfrm


class OgdchLanguagePlugin(plugins.SingletonPlugin):
    """
    Handles language dictionaries in data_dict (pkg_dict).
    """

    def before_view(self, pkg_dict):
        return self._prepare_group_or_org_json(pkg_dict)

    def _ignore_field(self, key):
        return False

    def _prepare_group_or_org_json(self, group_or_org_dict):
        # parse all json strings in dict
        group_or_org_dict = self._parse_json_strings(group_or_org_dict)

        # map ckan fields
        group_or_org_dict = self._group_or_org_map_ckan_default_fields(
            group_or_org_dict
        )

        # Do not change the resulting dict for API requests and form saves.
        # _reduce_to_requested_language removes all translation dicts needed
        # to show the form on resource_edit, so we skip it here
        if sh.request_is_api_request() or toolkit.request.method == "POST":
            return group_or_org_dict

        # replace langauge dicts with requested language strings
        desired_lang_code = sh.get_request_language()
        group_or_org_dict = self._reduce_to_requested_language(
            group_or_org_dict, desired_lang_code
        )

        return group_or_org_dict

    def _prepare_package_json(self, pkg_dict):
        # parse all json strings in dict
        pkg_dict = self._parse_json_strings(pkg_dict)

        # map ckan fields
        pkg_dict = self._package_map_ckan_default_fields(pkg_dict)

        return pkg_dict

    def _parse_json_strings(self, data_dict):
        # try to parse all values as JSON
        for key, value in list(data_dict.items()):
            data_dict[key] = sh.parse_json(value)

        # groups
        if "groups" in data_dict and data_dict["groups"] is not None:
            for group in data_dict["groups"]:
                for field in group:
                    group[field] = sh.parse_json(group[field])

        # organization
        if "organization" in data_dict and data_dict["organization"] is not None:
            for field in data_dict["organization"]:
                data_dict["organization"][field] = sh.parse_json(
                    data_dict["organization"][field]
                )

        return data_dict

    def _package_map_ckan_default_fields(self, pkg_dict):
        if "title" in pkg_dict:
            pkg_dict["display_name"] = pkg_dict["title"]

        if pkg_dict.get("maintainer") is None and pkg_dict.get("contact_points"):
            pkg_dict["maintainer"] = pkg_dict["contact_points"][0]["name"]
        if pkg_dict.get("maintainer_email") is None and pkg_dict.get("contact_points"):
            pkg_dict["maintainer_email"] = pkg_dict["contact_points"][0]["email"]

        if pkg_dict.get("author") is None and pkg_dict.get("publisher"):
            pkg_dict["author"] = pkg_dict["publisher"]["name"][
                sh.get_request_language()
            ]
        if "notes" in pkg_dict:
            del pkg_dict["notes"]

        if "resources" in pkg_dict and pkg_dict["resources"] is not None:
            for resource in pkg_dict["resources"]:
                if "title" in resource:
                    resource["name"] = resource["title"]
        return pkg_dict

    def _group_or_org_map_ckan_default_fields(self, group_or_org_dict):
        if "title" in group_or_org_dict:
            group_or_org_dict["display_name"] = group_or_org_dict["title"]
        if "notes" in group_or_org_dict:
            del group_or_org_dict["notes"]

        return group_or_org_dict

    def _extract_lang_value(self, value, lang_code):
        new_value = sh.parse_json(value)

        if isinstance(new_value, dict):
            return sh.get_localized_value(new_value, lang_code, default_value="")
        return value

    def _reduce_to_requested_language(self, pkg_dict, desired_lang_code):
        # pkg fields
        for key, value in list(pkg_dict.items()):
            if not self._ignore_field(key):
                pkg_dict[key] = self._extract_lang_value(value, desired_lang_code)

        return pkg_dict


class OgdchGroupPlugin(OgdchLanguagePlugin):
    plugins.implements(plugins.IGroupController, inherit=True)

    # IGroupController
    def before_view(self, pkg_dict):
        return super(OgdchGroupPlugin, self).before_view(pkg_dict)


class OgdchOrganizationPlugin(OgdchLanguagePlugin):
    plugins.implements(plugins.IOrganizationController, inherit=True)

    # IOrganizationController
    def before_view(self, pkg_dict):
        return super(OgdchOrganizationPlugin, self).before_view(pkg_dict)


class OgdchPackagePlugin(OgdchLanguagePlugin):
    plugins.implements(plugins.IPackageController, inherit=True)

    def is_supported_package_type(self, pkg_dict):
        # only package type 'dataset' is supported (not harvesters!)
        try:
            return pkg_dict["type"] == "dataset"
        except KeyError:
            return False

    # IPackageController
    def before_dataset_view(self, pkg_dict):
        """This is called before the dataset is displayed.

        Depending on caching, the pkg_dict passed here might be the one that gets sent
        to the template. However, we might get a pkg_dict that has not yet been
        validated against our schema - that means that the custom dataset fields are
        still in the 'extras' of the pkg_dict. See ckan.logic.action.get.package_show
        for full details.
        """
        if not self.is_supported_package_type(pkg_dict):
            return pkg_dict

        return self._prepare_package_json(pkg_dict)

    def after_dataset_show(self, context, pkg_dict):
        if not self.is_supported_package_type(pkg_dict):
            return pkg_dict

        pkg_dict = self._package_map_ckan_default_fields(pkg_dict)

        # groups
        if pkg_dict["groups"] is not None:
            for group in pkg_dict["groups"]:
                for field in group:
                    group[field] = sh.parse_json(group[field])

        # organization
        if pkg_dict["organization"] is not None:
            for field in pkg_dict["organization"]:
                pkg_dict["organization"][field] = sh.parse_json(
                    pkg_dict["organization"][field]
                )

        if sh.request_is_api_request() and not toolkit.request.method == "POST":
            # We want to convert datetimes to Europe/Zurich and include the time zone
            # information, but only when returning the dataset via the API, not when
            # handling it internally or updating the dataset.
            sh.convert_datetimes_for_api(pkg_dict)

        return pkg_dict

    def before_dataset_index(self, search_data):
        if not self.is_supported_package_type(search_data):
            return search_data

        extract_title = LangToString("title")
        validated_dict = json.loads(search_data["validated_data_dict"])

        search_data["res_name"] = [
            extract_title(r) for r in validated_dict["resources"]
        ]
        search_data["res_format"] = [
            r["format"] for r in validated_dict["resources"] if "format" in r
        ]
        search_data["res_rights"] = [
            sh.simplify_terms_of_use(r.get("rights", ""))
            for r in validated_dict["resources"]
        ]
        search_data["title_string"] = extract_title(validated_dict)
        search_data["description"] = LangToString("description")(validated_dict)

        sh.index_language_specific_values(search_data, validated_dict)

        sh.clean_up_list_fields(search_data, validated_dict)

        return search_data

    # borrowed from ckanext-multilingual (core extension)
    def before_dataset_search(self, search_params):
        """Search in correct language-specific field and boost results in current
        language
        """
        lang_set = sh.get_langs()
        try:
            current_lang = toolkit.request.environ["CKAN_LANG"]
        except (KeyError, RuntimeError):
            # This happens when this code gets called as part of a paster
            # command rather then as part of an HTTP request.
            current_lang = toolkit.config.get("ckan.locale_default")

        # fallback to default locale if locale not in suported langs
        if current_lang not in lang_set:
            current_lang = toolkit.config.get("ckan.locale_default", "en")
        # treat current lang differenly so remove from set
        lang_set.remove(current_lang)

        # add default query field(s)
        query_fields = "text"

        # weight current lang more highly
        query_fields += " title_%s^8 text_%s^4" % (current_lang, current_lang)

        for lang in lang_set:
            query_fields += " title_%s^2 text_%s" % (lang, lang)

        search_params["qf"] = query_fields + " res_name res_description"

        return search_params


class OgdchResourcePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IResourceController, inherit=True)

    # IResourceController

    def before_resource_create(self, context, resource):
        return self._set_resource_size_values(resource)

    def before_resource_update(self, context, current, resource):
        return self._set_resource_size_values(resource)

    def _set_resource_size_values(self, resource):
        upload = uploader.get_resource_uploader(resource)

        if hasattr(upload, "filesize"):
            resource["size"] = upload.filesize
            resource["byte_size"] = upload.filesize

        return resource


class LangToString(object):
    def __init__(self, attribute):
        self.attribute = attribute

    def __call__(self, data_dict):
        try:
            lang = data_dict[self.attribute]

            return "%s - %s - %s - %s" % (
                lang.get("de", ""),
                lang.get("fr", ""),
                lang.get("it", ""),
                lang.get("en", ""),
            )
        except KeyError:
            return ""
        except AttributeError:
            return None
