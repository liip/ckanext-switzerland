# encoding: utf-8
import datetime
import json
import logging
from zoneinfo import ZoneInfo

import ckan.tests.helpers as helpers
import pytest
import time_machine
from bs4 import BeautifulSoup
from ckan.lib.helpers import url_for

from . import data

log = logging.getLogger(__name__)


@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
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
        user = data.user()
        dataset = data.dataset()
        resource = data.resource(dataset=dataset)

        dataset.update(**dataset_datetime_fields)
        resource.update(**resource_datetime_fields)

        helpers.call_action("package_update", {"user": user["name"]}, **dataset)
        helpers.call_action("resource_create", {"user": user["name"]}, **resource)

    def _get_resource_page(self, app):
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

        return app.get(
            url_for(
                "resource.read", id="dataset", resource_id=resource_id, qualified=True
            )
        )

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

    def test_get_correct_datetime_format_for_dataset_display(self, app):
        self._create_dataset()

        resp = app.get(url_for("dataset.read", id="dataset", qualified=True))
        soup = BeautifulSoup(resp.body, "html.parser")

        # All dates should be in UTC.
        # All dates should be passed to the automatic-local-datetime function, which
        # displays them as a date with time and timezone info.
        issued = soup.find("th", text="Issued date").findNext("td").find("span")
        assert issued["class"][0] == "automatic-local-datetime"
        assert issued["data-datetime"] == "2022-04-18T12:00:00+0000"

        modified = soup.find("th", text="Modified date").findNext("td").find("span")
        assert modified["class"][0] == "automatic-local-datetime"
        assert modified["data-datetime"] == "2022-04-18T12:30:00+0000"

    def test_get_correct_datetime_format_for_resource_display(self, app):
        self._create_dataset()
        resp = self._get_resource_page(app)
        soup = BeautifulSoup(resp.body, "html.parser")

        # All dates should be in UTC.
        # All dates should be passed to the automatic-local-datetime function, which
        # displays them as a date with time and timezone info.
        data_updated = (
            soup.find("th", text="Data last updated").findNext("td").find("span")
        )
        assert data_updated["class"][0] == "automatic-local-datetime"
        assert data_updated["data-datetime"] == "2022-04-18T12:30:00+0000"

        metadata_updated = (
            soup.find("th", text="Metadata last updated").findNext("td").find("span")
        )
        assert metadata_updated["class"][0] == "automatic-local-datetime"
        assert metadata_updated["data-datetime"] == "2022-04-20T14:15:00+0000"

        created = soup.find("th", text="Created").findNext("td").find("span")
        assert created["class"][0] == "automatic-local-datetime"
        assert created["data-datetime"] == "2022-04-20T14:15:00+0000"

    def test_get_correct_url_for_ogdch_home_search_rule(self, app):
        url = url_for("ogdch_home.search")

        assert url == "/"

    def test_get_dataset_search_page_for_home_url(self, app):
        self._create_dataset()

        resp = app.get("/")
        soup = BeautifulSoup(resp.body, "html.parser")

        assert soup.find("li", class_="active").text == "Datasets"
        assert "1 dataset found" in soup.find("h1").text

    def test_get_correct_fields_for_dataset_page(self, app):
        self._create_dataset()
        resp = app.get(url_for("dataset.read", id="dataset", qualified=True))
        soup = BeautifulSoup(resp.body, "html.parser")

        expected_dataset_fields = [
            "Identifier",
            "Slug",
            "Issued date",
            "Modified date",
            "Permalink",
            "Organization",
            "Publishers",
            "Contact points",
            "Keywords",
            "Further information",
            "Temporal coverage",
            "Update interval",
            "Landing page",
            "Languages",
            "Terms of use",
        ]

        table = soup.find(
            "table", class_="table table-striped table-bordered table-condensed"
        )
        actual_dataset_fields = [th.text for th in table.find_all("th", scope="row")]
        assert actual_dataset_fields == expected_dataset_fields

    def test_get_correct_fields_for_resource_page(self, app):
        self._create_dataset()
        resp = self._get_resource_page(app)
        soup = BeautifulSoup(resp.body, "html.parser")

        expected_resource_fields = [
            "Permalink",
            "Data last updated",
            "Metadata last updated",
            "Created",
            "Format",
            "Identifier",
            "Title",
            "Media type",
            "Media type (inner)",
            "Coverage",
            "File size",
            "ID",
        ]

        table = soup.find(
            "table", class_="table table-striped table-bordered table-condensed"
        )
        actual_resource_fields = [th.text for th in table.find_all("th", scope="row")]
        assert actual_resource_fields == expected_resource_fields
