# encoding: utf-8
import datetime
import json
import logging
from zoneinfo import ZoneInfo

import ckan.tests.helpers as helpers
import pytest
import responses
import time_machine
from bs4 import BeautifulSoup
from ckan.lib.helpers import url_for

from . import data

log = logging.getLogger(__name__)


@pytest.mark.ckan_config(
    "ckan.plugins", "ogdch ogdch_pkg harvest fluent scheming_datasets activity"
)
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
        # These are our custom values that we set on the dataset, in Europe/Zurich time
        # zone and without time zone info.
        dataset_datetime_fields = {
            "issued": "2022-04-20T12:00:00",
            "modified": "2022-04-20T12:30:00",
            "version": "2022-04-20T12:30:00",
        }
        # last_modified is a CKAN default field, but we can set its value, unlike
        # metadata_modified and metadata_created.
        # issued and modified are our custom values.
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

    def _set_up_responses(self):
        """Add the urls that we need to mock to the responses registry, with the mocked
        response.

        Add a passthru for every url prefix we don't want to mock (e.g. solr).
        """
        responses.add(
            responses.GET,
            "http://wp.test?action=get_nav&lang=en",
            content_type="text/css",
            status=200,
            json={
                "success": True,
                "data": {
                    "main": "",
                    "admin": "",
                    "footer": "",
                    "user": "",
                    "title": "",
                    "css": "",
                },
            },
        )
        responses.add_passthru("http://solr")

    def test_get_correct_datetime_format_from_api(self, app):
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

    @responses.activate
    def test_get_correct_datetime_format_for_dataset_display(self, app):
        self._set_up_responses()
        self._create_dataset()

        resp = app.get(url_for("dataset.read", id="dataset", qualified=True))
        soup = BeautifulSoup(resp.body, "html.parser")

        last_updated = soup.find("dt", text="Last updated").findNext("dd").find("span")
        issued = soup.find("dt", text="Issued date").findNext("dd").find("span")
        modified = soup.find("dt", text="Modified date").findNext("dd").find("span")

        # These values should be in UTC
        assert (
            last_updated["data-datetime"]
            == modified["data-datetime"]
            == "2022-04-20T10:30:00+0000"
        )
        assert issued["data-datetime"] == "2022-04-20T10:00:00+0000"

    @responses.activate
    def test_get_correct_datetime_format_for_resource_display(self, app):
        self._set_up_responses()
        self._create_dataset()

        pkg_resp = app.get(
            url_for(
                "api.action",
                logic_function="package_show",
                ver=3,
                id="dataset",
                status=200,
            )
        )
        pkg_dict = json.loads(pkg_resp.body)["result"]
        resource_id = pkg_dict["resources"][0]["id"]

        resp = app.get(
            url_for(
                "resource.read", id="dataset", resource_id=resource_id, qualified=True
            )
        )
        soup = BeautifulSoup(resp.body, "html.parser")

        # modified date should be in UTC
        modified = soup.find("dt", text="Modified date").findNext("dd").find("span")
        assert modified["data-datetime"] == "2022-04-20T10:30:00+0000"
