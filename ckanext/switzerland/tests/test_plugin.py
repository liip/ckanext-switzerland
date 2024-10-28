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
        All datetimes are saved in the database and Solr as UTC.

        Fields that CKAN sets to the current UTC time when creating/updating a dataset:
        - dataset metadata_created
        - dataset metadata_modified
        - resource created
        - resource last_modified
        - resource metadata_modified
        """
        # Fields where we set a value directly when creating/updating a dataset/resource
        dataset_datetime_fields = {
            "issued": "2022-04-18T12:00:00",
            "modified": "2022-04-18T12:30:00",
            "version": "2022-04-18T12:30:00",
        }
        resource_datetime_fields = {
            "last_modified": "2022-04-18T12:30:00",
            "issued": "2022-04-18T12:00:00",
            "modified": "2022-04-18T12:30:00",
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
        assert pkg_dict["issued"] == "2022-04-18T14:00:00+02:00"
        assert pkg_dict["modified"] == "2022-04-18T14:30:00+02:00"
        assert pkg_dict["version"] == "2022-04-18T14:30:00+02:00"

        assert resource_dict["created"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["metadata_modified"] == "2022-04-20T16:15:00+02:00"
        assert resource_dict["last_modified"] == "2022-04-18T14:30:00+02:00"
        assert resource_dict["issued"] == "2022-04-18T14:00:00+02:00"
        assert resource_dict["modified"] == "2022-04-18T14:30:00+02:00"

    @responses.activate
    def test_get_correct_datetime_format_for_dataset_display(self, app):
        self._set_up_responses()
        self._create_dataset()

        resp = app.get(url_for("dataset.read", id="dataset", qualified=True))
        soup = BeautifulSoup(resp.body, "html.parser")

        issued = soup.find("th", text="Issued date").findNext("td").find("span")
        modified = soup.find("th", text="Modified date").findNext("td").find("span")

        # These values should be in UTC
        assert modified["data-datetime"] == "2022-04-18T12:30:00+0000"
        assert issued["data-datetime"] == "2022-04-18T12:00:00+0000"

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
        modified = soup.find("th", text="Modified date").findNext("td").find("span")
        assert modified["data-datetime"] == "2022-04-18T12:30:00+0000"

    def test_get_correct_url_for_ogdch_home_search_rule(self, app):
        url = url_for("ogdch_home.search")

        assert url == "/"
