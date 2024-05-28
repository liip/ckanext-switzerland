# encoding: utf-8
import datetime
import json
import logging
from zoneinfo import ZoneInfo

import ckan.tests.helpers as helpers
import pytest
import time_machine
from ckan.lib.helpers import url_for

from . import data

log = logging.getLogger(__name__)


@pytest.mark.usefixtures("clean_db", "clean_index", "with_plugins")
class TestOgdchPackagePlugin(object):
    @time_machine.travel(
        datetime.datetime(2022, 4, 20, 14, 15, 0, 0, ZoneInfo("UTC")), tick=False
    )
    def _create_dataset(self):
        """Create a dataset with a resource and set datetime fields to known values.

        We mock the current time as 14:15 UTC, because CKAN sets the values for
        'metadata_created' and 'metadata_modified' to datetime.datetime.utcnow() when
        creating or modifying a dataset/resource.
        """
        dataset_datetime_fields = {
            "issued": "2022-04-20T12:00:00",
            "modified": "2022-04-20T12:30:00",
            "version": "2022-04-20T12:30:00",
        }
        resource_datetime_fields = {
            "last_modified": "2022-04-20T14:15:00",
            "issued": "2022-04-20T12:00:00",
            "modified": "2022-04-20T12:30:00",
        }
        dataset = data.dataset()
        resource = data.resource(dataset=dataset)

        dataset.update(**dataset_datetime_fields)
        resource.update(**resource_datetime_fields)

        helpers.call_action("package_update", **dataset)
        helpers.call_action("resource_create", **resource)

    def test_get_correct_datetime_format_from_api(self, app, ckan_config):
        self._create_dataset()

        resp = app.get(
            url_for(
                "api.action",
                logic_function="package_show",
                ver=3,
                id="dataset",
                status=200,
            )
        )
        pkg_dict = json.loads(resp.body)["result"]
        resource_dict = pkg_dict["resources"][0]

        assert pkg_dict["metadata_created"] == "2022-04-20T16:15:00+02:00"
        assert pkg_dict["metadata_modified"] == "2022-04-20T16:15:00+02:00"
        assert pkg_dict["issued"] == "2022-04-20T12:00:00+02:00"
        assert pkg_dict["modified"] == "2022-04-20T12:30:00+02:00"
        assert pkg_dict["version"] == "2022-04-20T12:30:00+02:00"

        assert resource_dict["created"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["metadata_modified"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["last_modified"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["issued"] == "2022-04-20T12:00:00+02:00"
        assert resource_dict["modified"] == "2022-04-20T12:30:00+02:00"
