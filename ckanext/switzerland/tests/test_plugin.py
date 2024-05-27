# encoding: utf-8
import json
import logging
import pytest

import ckan.tests.helpers as helpers
from ckan.lib.helpers import url_for

from . import data

log = logging.getLogger(__name__)


@pytest.mark.usefixtures("clean_db", "clean_index", "with_plugins")
class TestOgdchPackagePlugin(object):

    def test_get_correct_datetime_format_from_api(self, app, ckan_config):
        dataset_datetime_fields = {
            "metadata_created": "2022-04-20T14:15:00",
            "metadata_modified": "2022-04-20T14:30:00",
            "issued": "2022-04-20T16:15:00",
            "modified": "2022-04-20T16:30:00",
            "version": "2022-04-20T16:30:00",
        }
        resource_datetime_fields = {
            "created": "2022-04-20T14:15:00",
            "last_modified": "2022-04-20T14:30:00",
            "metadata_modified": "2022-04-20T14:30:00",
            "issued": "2022-04-20T16:15:00",
            "modified": "2022-04-20T16:30:00",
        }
        dataset = data.dataset()
        resource = data.resource(dataset=dataset)

        dataset.update(**dataset_datetime_fields)
        resource.update(**resource_datetime_fields)

        helpers.call_action('package_update', **dataset)
        helpers.call_action('resource_create', **resource)

        resp = app.get(
            url_for(
                "api.action",
                logic_function="package_show",
                ver=3,
                id=dataset["name"],
                status=200
            )
        )
        pkg_dict = json.loads(resp.body)["result"]
        resource_dict = pkg_dict["resources"][0]

        # Fields set by CKAN
        assert pkg_dict["metadata_created"] == "2022-04-20T16:15:00+02:00"
        assert pkg_dict["metadata_modified"] == "2022-04-20T16:30:00+02:00"
        # Our custom fields
        assert pkg_dict["issued"] == "2022-04-20T16:15:00+02:00"
        assert pkg_dict["modified"] == "2022-04-20T16:30:00+02:00"
        assert pkg_dict["version"] == "2022-04-20T16:30:00+02:00"

        # Fields set by CKAN
        assert resource_dict["created"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["last_modified"] == "2022-04-20T16:30:00+02:00"
        assert resource_dict["metadata_modified"] == "2022-04-20T16:30:00+02:00"
        # Our custom fields
        assert resource_dict["issued"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["modified"] == "2022-04-20T16:30:00+02:00"
