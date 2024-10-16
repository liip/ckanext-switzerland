import unittest

from ckan.lib.navl.dictization_functions import Invalid

from ckanext.switzerland.tests import data
from ckanext.switzerland.validators import ogdch_name_validator


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
