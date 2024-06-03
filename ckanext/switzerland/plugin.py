# coding=UTF-8
import json
import logging
import os
import sys

import ckan.lib.helpers as h
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.munge import munge_title_to_name

import ckanext.switzerland.helpers as sh
from ckanext.switzerland import logic as l
from ckanext.switzerland import validators as v
from ckanext.switzerland.blueprints import ogdch_admin, ogdch_dataset, ogdch_resource

log = logging.getLogger(__name__)


class OgdchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IValidators)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IBlueprint, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_resource("assets", "switzerland")

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
        }

    # IActions

    def get_actions(self):
        """
        Expose new API methods
        """
        return {
            "ogdch_dataset_count": l.ogdch_dataset_count,
            "ogdch_dataset_terms_of_use": l.ogdch_dataset_terms_of_use,
            "ogdch_dataset_by_identifier": l.ogdch_dataset_by_identifier,
            "ogdch_content_headers": l.ogdch_content_headers,
        }

    # ITemplateHelpers

    def get_helpers(self):
        """
        Provide template helper functions
        """
        return {
            "get_dataset_count": sh.get_dataset_count,
            "get_group_count": sh.get_group_count,
            "get_app_count": sh.get_app_count,
            "get_org_count": sh.get_org_count,
            "get_tweet_count": sh.get_tweet_count,
            "get_localized_org": sh.get_localized_org,
            "get_localized_value": sh.get_localized_value,
            "localize_json_title": sh.localize_json_title,
            "parse_and_localize": sh.parse_and_localize,
            "get_frequency_name": sh.get_frequency_name,
            "get_terms_of_use_icon": sh.get_terms_of_use_icon,
            "get_dataset_terms_of_use": sh.get_dataset_terms_of_use,
            "get_dataset_by_identifier": sh.get_dataset_by_identifier,
            "get_readable_file_size": sh.get_readable_file_size,
            "get_matomo_config": sh.get_matomo_config,
            "parse_json": sh.parse_json,
            "convert_post_data_to_dict": sh.convert_post_data_to_dict,
            "resource_filename": sh.resource_filename,
            "load_wordpress_templates": sh.load_wordpress_templates,
            "render_description": sh.render_description,
            "get_resource_display_items": sh.get_resource_display_items,
            "convert_datetimes_for_api": sh.convert_datetimes_for_api,
            "convert_datetimes_for_display": sh.convert_datetimes_for_display,
            "request_is_api_request": sh.request_is_api_request,
            # monkey patch template helpers to return translated names/titles
            "dataset_display_name": sh.dataset_display_name,
            "resource_display_name": sh.resource_display_name,
            "group_link": sh.group_link,
            "resource_link": sh.resource_link,
            "organization_link": sh.organization_link,
            "get_langs": sh.get_langs,
            "localize_change_dict": sh.localize_change_dict,
            "get_cookie_law_url": sh.get_cookie_law_url,
            "get_cookie_law_id": sh.get_cookie_law_id,
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
        return [ogdch_admin, ogdch_dataset]


# monkey patch template helpers to return translated names/titles
h.dataset_display_name = sh.dataset_display_name
h.resource_display_name = sh.resource_display_name
h.group_link = sh.group_link
h.resource_link = sh.resource_link
h.organization_link = sh.organization_link


class OgdchLanguagePlugin(plugins.SingletonPlugin):
    """
    Handles language dictionaries in data_dict (pkg_dict).
    """

    def before_view(self, pkg_dict):
        return self._prepare_package_json(pkg_dict)

    def _ignore_field(self, key):
        return False

    def _prepare_package_json(self, pkg_dict):
        # parse all json strings in dict
        pkg_dict = self._package_parse_json_strings(pkg_dict)

        # map ckan fields
        pkg_dict = self._package_map_ckan_default_fields(pkg_dict)

        try:
            # Do not change the resulting dict for API requests and form saves
            # _package_reduce_to_requested_language removes all translation dicts needed
            # to show the form on resource_edit, so we skip it here
            path = toolkit.request.path
            if path.startswith("/api") or toolkit.request.path == "POST":
                return pkg_dict
        except TypeError:
            # we get here if there is no request (i.e. on the command line)
            return pkg_dict

        # replace langauge dicts with requested language strings
        desired_lang_code = self._get_request_language()
        pkg_dict = self._package_reduce_to_requested_language(
            pkg_dict, desired_lang_code
        )

        return pkg_dict

    def _get_request_language(self):
        try:
            return toolkit.request.environ["CKAN_LANG"]
        except TypeError:
            return toolkit.config.get("ckan.locale_default", "en")

    def _package_parse_json_strings(self, pkg_dict):
        # try to parse all values as JSON
        for key, value in list(pkg_dict.items()):
            pkg_dict[key] = sh.parse_json(value)

        # groups
        if "groups" in pkg_dict and pkg_dict["groups"] is not None:
            for group in pkg_dict["groups"]:
                """
                TODO: somehow the title is messed up here,
                but the display_name is okay
                """
                group["title"] = group["display_name"]
                for field in group:
                    group[field] = sh.parse_json(group[field])

        # organization
        if "organization" in pkg_dict and pkg_dict["organization"] is not None:
            for field in pkg_dict["organization"]:
                pkg_dict["organization"][field] = sh.parse_json(
                    pkg_dict["organization"][field]
                )

        return pkg_dict

    def _package_map_ckan_default_fields(self, pkg_dict):
        if "title" in pkg_dict:
            pkg_dict["display_name"] = pkg_dict["title"]
        if "contact_points" in pkg_dict and pkg_dict["contact_points"] is not None:
            if pkg_dict["maintainer"] is None:
                pkg_dict["maintainer"] = pkg_dict["contact_points"][0]["name"]

            if pkg_dict["maintainer_email"] is None:
                pkg_dict["maintainer_email"] = pkg_dict["contact_points"][0]["email"]
        if "publishers" in pkg_dict and pkg_dict["publishers"] is not None:
            if pkg_dict["author"] is None:
                pkg_dict["author"] = pkg_dict["publishers"][0]["label"]

        if "resources" in pkg_dict and pkg_dict["resources"] is not None:
            for resource in pkg_dict["resources"]:
                if "title" in resource:
                    resource["name"] = resource["title"]
        return pkg_dict

    def _extract_lang_value(self, value, lang_code):
        new_value = sh.parse_json(value)

        if isinstance(new_value, dict):
            return sh.get_localized_value(new_value, lang_code, default_value="")
        return value

    def _package_reduce_to_requested_language(self, pkg_dict, desired_lang_code):
        # pkg fields
        for key, value in list(pkg_dict.items()):
            if not self._ignore_field(key):
                pkg_dict[key] = self._extract_lang_value(value, desired_lang_code)

        # groups
        pkg_dict = self._reduce_group_language(pkg_dict, desired_lang_code)

        # organization
        pkg_dict = self._reduce_org_language(pkg_dict, desired_lang_code)

        # resources
        pkg_dict = self._reduce_res_language(pkg_dict, desired_lang_code)

        return pkg_dict

    def _reduce_group_language(self, pkg_dict, desired_lang_code):
        if "groups" in pkg_dict and pkg_dict["groups"] is not None:
            try:
                for element in pkg_dict["groups"]:
                    for field in element:
                        element[field] = self._extract_lang_value(
                            element[field], desired_lang_code
                        )
            except TypeError:
                pass

        return pkg_dict

    def _reduce_org_language(self, pkg_dict, desired_lang_code):
        if "organization" in pkg_dict and pkg_dict["organization"] is not None:
            try:
                for field in pkg_dict["organization"]:
                    pkg_dict["organization"][field] = self._extract_lang_value(
                        pkg_dict["organization"][field], desired_lang_code
                    )
            except TypeError:
                pass
        return pkg_dict

    def _reduce_res_language(self, pkg_dict, desired_lang_code):
        if "resources" in pkg_dict and pkg_dict["resources"] is not None:
            try:
                for element in pkg_dict["resources"]:
                    for field in element:
                        element[field] = self._extract_lang_value(
                            element[field], desired_lang_code
                        )
            except TypeError:
                pass
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


class OgdchResourcePlugin(OgdchLanguagePlugin):
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IBlueprint, inherit=True)

    # IResourceController

    def _ignore_field(self, key):
        return key == "tracking_summary"

    # IBlueprint

    def get_blueprint(self):
        return ogdch_resource


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

        sh.convert_datetimes_for_display(pkg_dict)

        return super(OgdchPackagePlugin, self).before_view(pkg_dict)

    def after_dataset_show(self, context, pkg_dict):
        if not self.is_supported_package_type(pkg_dict):
            return pkg_dict

        pkg_dict = self._package_map_ckan_default_fields(pkg_dict)

        # groups
        if pkg_dict["groups"] is not None:
            for group in pkg_dict["groups"]:
                """
                TODO: somehow the title is messed up here,
                but the display_name is okay
                """
                group["title"] = group["display_name"]
                for field in group:
                    group[field] = sh.parse_json(group[field])

        # organization
        if pkg_dict["organization"] is not None:
            for field in pkg_dict["organization"]:
                pkg_dict["organization"][field] = sh.parse_json(
                    pkg_dict["organization"][field]
                )

        if sh.request_is_api_request():
            # We want to convert datetimes to Europe/Zurich and include the time zone
            # information, but only when returning the dataset via the API, not when
            # handling it internally.
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
            r["media_type"] for r in validated_dict["resources"] if "media_type" in r
        ]
        search_data["res_rights"] = [
            sh.simplify_terms_of_use(r.get("rights", ""))
            for r in validated_dict["resources"]
        ]
        search_data["title_string"] = extract_title(validated_dict)
        search_data["description"] = LangToString("description")(validated_dict)

        try:
            # index language-specific values (or it's fallback)
            text_field_items = {}
            for lang_code in sh.get_langs():
                search_data["title_" + lang_code] = sh.get_localized_value(
                    validated_dict["title"], lang_code
                )
                search_data["title_string_" + lang_code] = munge_title_to_name(
                    sh.get_localized_value(validated_dict["title"], lang_code)
                )
                search_data["description_" + lang_code] = sh.get_localized_value(
                    validated_dict["description"], lang_code
                )

                search_data["keywords_" + lang_code] = validated_dict["keywords"].get(
                    lang_code
                )

                text_field_items["text_" + lang_code] = [
                    sh.get_localized_value(validated_dict["description"], lang_code)
                ]
                if search_data["keywords_" + lang_code]:
                    text_field_items["text_" + lang_code].append(
                        " ".join(search_data["keywords_" + lang_code])
                    )

            # flatten values for text_* fields
            for key, value in list(text_field_items.items()):
                search_data[key] = " ".join(value)

        except KeyError:
            pass

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

        # weight current lang more highly
        query_fields = "title_%s^8 text_%s^4" % (current_lang, current_lang)

        for lang in lang_set:
            query_fields += " title_%s^2 text_%s" % (lang, lang)

        search_params["qf"] = query_fields + " res_name res_description"

        """
        Unless the query is already being filtered by any type
        (either positively, or negatively), reduce to only display
        'dataset' type
        This is done because by standard all types are displayed, this
        leads to strange situations where e.g. harvest sources are shown
        on organization pages.
        TODO: fix issue https://github.com/ckan/ckan/issues/2803 in CKAN core
        """
        fq = search_params.get("fq", "")
        if "dataset_type:" not in fq:
            search_params.update({"fq": "%s +dataset_type:dataset" % fq})

        return search_params


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
