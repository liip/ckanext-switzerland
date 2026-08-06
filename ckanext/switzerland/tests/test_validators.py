import json
import unittest

from ckan.lib.navl.dictization_functions import Invalid
from ckan.plugins.toolkit import missing

from ckanext.switzerland.tests import data
from ckanext.switzerland.validators import ogdch_fluent_tags, ogdch_name_validator


class TestNameValidator(unittest.TestCase):
    def test_valid_ckan_username(self):
        # CKAN usernames are only allowed to contain a-z, 0-9 and -_
        value = "valid_username-1"
        context = {"user_obj": data.user()}

        self.assertEqual(value, ogdch_name_validator(value, context))

    def test_valid_existing_username(self):
        # Existing usernames (created on WordPress) are also allowed to include capital
        # letters, a space and the @ character
        value = "Valid_User Name@1"
        context = {"user_obj": data.user()}

        self.assertEqual(value, ogdch_name_validator(value, context))

    def test_invalid_existing_username(self):
        # Even existing usernames are not allowed to include characters outside of
        # a-z, A-Z, 0-9, space and -_@
        value = "Invalid_Username;#´"
        context = {"user_obj": data.user()}

        with self.assertRaisesRegex(Invalid, "Must be purely alphanumeric"):
            ogdch_name_validator(value, context)

    def test_valid_new_username(self):
        # New usernames have to conform to CKAN validation: only a-z, 0-9 and -_
        value = "valid_username-1"
        context = {}

        self.assertEqual(value, ogdch_name_validator(value, context))

    def test_invalid_new_username_that_would_be_valid_existing_username(self):
        # New usernames have to conform to CKAN validation: only a-z, 0-9 and -_
        value = "Invalid_User Name@1"
        context = {}

        with self.assertRaisesRegex(Invalid, "Must be purely lowercase alphanumeric"):
            ogdch_name_validator(value, context)

    def test_invalid_new_username(self):
        # New usernames have to conform to CKAN validation: only a-z, 0-9 and -_
        value = "Invalid_Username;#´"
        context = {}

        with self.assertRaisesRegex(Invalid, "Must be purely lowercase alphanumeric"):
            ogdch_name_validator(value, context)


class TestOgdchFluentTags(unittest.TestCase):
    def _validator(self):
        field = {"field_name": "keywords"}
        schema = {"form_languages": ["de", "fr", "it", "en"]}
        return ogdch_fluent_tags(field, schema)

    def _empty_keywords_json(self):
        return json.dumps({"de": [], "fr": [], "it": [], "en": []})

    def test_missing_value_becomes_empty_lists_per_language(self):
        key = ("keywords",)
        data = {key: missing}
        errors = {key: []}
        self._validator()(key, data, errors, {})
        self.assertEqual(data[key], self._empty_keywords_json())

    def test_none_value_becomes_empty_lists_per_language(self):
        key = ("keywords",)
        data = {key: None}
        errors = {key: []}
        self._validator()(key, data, errors, {})
        self.assertEqual(data[key], self._empty_keywords_json())

    def test_partial_tags_fill_missing_languages(self):
        key = ("keywords",)
        data = {key: json.dumps({"de": ["Foo Tag"]})}
        errors = {key: []}
        self._validator()(key, data, errors, {})
        self.assertEqual(
            json.loads(data[key]),
            {"de": ["foo-tag"], "fr": [], "it": [], "en": []},
        )

    def test_dict_value_is_accepted(self):
        key = ("keywords",)
        data = {key: {"de": ["Bar"], "fr": []}}
        errors = {key: []}
        self._validator()(key, data, errors, {})
        self.assertEqual(
            json.loads(data[key]),
            {"de": ["bar"], "fr": [], "it": [], "en": []},
        )
