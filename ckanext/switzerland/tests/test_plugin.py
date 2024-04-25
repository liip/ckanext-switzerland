# encoding: utf-8
import json
import logging
import pytest

import ckan.tests.helpers as helpers
from ckan.lib.helpers import url_for
from unittest import

from . import data

log = logging.getLogger(__name__)


@pytest.mark.usefixtures("clean_db", "clean_index", "with_plugins")
class TestOgdchPackagePlugin(object):

    def test_get_correct_datetime_format_from_api(self, app, ckan_config):
        datetime_fields = {
            # "created": "2022-04-20T14:15:00.000000",
            # "last_modified": "2022-04-20T14:30:00.000000",
            "metadata_created": "2022-04-20T14:15:00.000000",
            "metadata_modified": "2022-04-20T14:30:00.000000",
            "issued": "2022-04-20T16:15:00.000000",
            "modified": "2022-04-20T16:30:00.000000",
            "version": "2022-04-20T16:30:00.000000",
        }
        dataset = data.dataset()

        log.warning(dataset)
        dataset.update(**datetime_fields)
        log.warning(dataset)

        resp = app.get(url_for("api.action", logic_function="package_show", ver=3, id=dataset["name"], status=200))
        pkg_dict = json.loads(resp.body)["result"]
        log.warning(pkg_dict)
        # assert resp["created"] == "2022-04-20T16:15:00.000000+02:00"
        # assert resp["last_modified"] == "2022-04-20T16:30:00.000000+02:00"
        assert pkg_dict["metadata_created"] == "2022-04-20T16:15:00.000000+02:00"
        assert pkg_dict["metadata_modified"] == "2022-04-20T16:30:00.000000+02:00"
        assert pkg_dict["issued"] == "2022-04-20T16:15:00.000000+02:00"
        assert pkg_dict["modified"] == "2022-04-20T16:30:00.000000+02:00"
        assert pkg_dict["version"] == "2022-04-20T16:30:00.000000+02:00"
