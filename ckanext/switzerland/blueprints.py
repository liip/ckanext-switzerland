import csv
import logging
from io import StringIO
from typing import Optional, Union

import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.lib.uploader as uploader
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import flask
import requests
from ckan.common import _, c, current_user, request
from ckan.lib import signals
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.lib.plugins import lookup_package_plugin
from ckan.types import Context, Response
from flask import Blueprint, make_response
from werkzeug.wrappers.response import Response as WerkzeugResponse

from ckanext.switzerland.helpers import resource_filename

log = logging.getLogger(__name__)
render = toolkit.render
abort = toolkit.abort
redirect = toolkit.redirect_to
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
ValidationError = logic.ValidationError
check_access = logic.check_access
get_action = logic.get_action
tuplize_dict = logic.tuplize_dict
clean_dict = logic.clean_dict
parse_params = logic.parse_params
flatten_to_string_key = logic.flatten_to_string_key
lookup_package_plugin = lookup_package_plugin

ogdch_admin = Blueprint("ogdch_admin", __name__, url_prefix="/ckan-admin")
ogdch_dataset = Blueprint("ogdch_dataset", __name__, url_prefix="/dataset")
ogdch_resource = Blueprint("ogdch_resource", __name__)


def email_address_exporter():
    """Returns a CSV of all Wordpress users: first name, last name and email address.
    Does not return information about CKAN users.
    """
    if not (c.userobj and c.userobj.sysadmin):
        abort(401, _("Unauthorized"))

    if "filter" in request.args:
        fobj = StringIO()
        writer = csv.writer(fobj)
        writer.writerow(["First Name", "Last Name", "Email"])

        wp_url = toolkit.config.get("ckanext.switzerland.wp_url")
        api_key = toolkit.config.get("ckanext.switzerland.user_list_api_key")
        url = "{}/wp-admin/admin-post.php?action=user_list&key={}".format(
            wp_url, api_key
        )
        users = requests.get(url).json()["data"]

        if request.args["filter"] != "all":
            followers = get_action("dataset_follower_list")(
                {}, {"id": request.args["filter"]}
            )
            followers = {follower["name"] for follower in followers}
            users = [u for u in users if u["user_login"] in followers]

        for user in users:
            writer.writerow([user["first_name"], user["last_name"], user["user_email"]])

        response = make_response(fobj.getvalue())
        response.headers["Content-Type"] = "text/csv"
        response.headers["Content-Disposition"] = 'attachment; filename="emails.csv"'

        return response

    packages = get_action("package_search")({}, {"sort": "name asc", "rows": 1000})[
        "results"
    ]
    for package in packages:
        try:
            package["follower_count"] = get_action("dataset_follower_count")(
                {}, {"id": package["id"]}
            )
        except ValidationError as e:
            log.error(
                f"Error getting follower count from dataset {package['name']} "
                f"with id {package['id']}: {e}"
            )

    c.datasets = packages
    return render("email_exporter/email_exporter.html")


def resource_download(
    id: str, resource_id: str, filename: Optional[str] = None
) -> Union[Response, WerkzeugResponse]:
    """
    Provides a direct download by either redirecting the user to the url
    stored or downloading an uploaded file directly.

    Copied from ckan 2.10 source code and changed to

    1. allow download of deleted resources:
        - rsc = get_action('resource_show')(context, {'id': resource_id})
        + resource_obj = model.Resource.get(resource_id)
        + rsc = resource_dictize(resource_obj, {'model': model})
    2. set Content-Disposition: attachment header so that resource files are
       downloaded, not opened in browser
    """
    context: Context = {"user": current_user.name, "auth_user_obj": current_user}

    try:
        resource_obj = model.Resource.get(resource_id)
        rsc = resource_dictize(resource_obj, {"model": model})
        get_action("package_show")(context, {"id": id})
    except NotFound:
        return base.abort(404, _("Resource not found"))
    except NotAuthorized:
        return base.abort(403, _("Not authorized to download resource"))

    if rsc.get("url_type") == "upload":
        upload = uploader.get_resource_uploader(rsc)
        filepath = upload.get_path(rsc["id"])
        resp = flask.send_file(filepath, download_name=filename)

        if rsc.get("mimetype"):
            resp.headers["Content-Type"] = rsc["mimetype"]
        resp.headers["Content-Disposition"] = "attachment"
        signals.resource_download.send(resource_id)
        return resp

    elif "url" not in rsc:
        return base.abort(404, _("No download is available"))
    return h.redirect_to(rsc["url"])


def resource_permalink(id, filename):
    context = {
        "model": model,
        "session": model.Session,
        "user": c.user or c.author,
        "for_view": True,
        "auth_user_obj": c.userobj,
    }
    data_dict = {"id": id, "include_tracking": True}

    try:
        dataset = get_action("package_show")(context, data_dict)
    except NotFound:
        abort(404, _("Dataset not found"))
    except NotAuthorized:
        abort(401, _("Unauthorized to read package %s") % id)

    for res in dataset["resources"]:
        if resource_filename(res["url"]) == filename:
            return redirect(res["url"])

    abort(404, _("Resource not found"))


def dataset_permalink(id):
    context = {
        "model": model,
        "session": model.Session,
        "user": c.user or c.author,
        "for_view": True,
        "auth_user_obj": c.userobj,
    }
    data_dict = {"id": id, "include_tracking": True}
    try:
        dataset = get_action("package_show")(context, data_dict)
    except NotFound:
        abort(404, _("Dataset not found"))
    except NotAuthorized:
        abort(401, _("Unauthorized to read package %s") % id)

    if not dataset["permalink"]:
        abort(404, _("Resource not found"))

    return redirect(dataset["permalink"])


def search():
    return render("search/search.html")


ogdch_admin.add_url_rule("/email_exporter", view_func=email_address_exporter)

ogdch_dataset.add_url_rule(
    "/<id>/resource/<resource_id>/download", view_func=resource_download
)
ogdch_dataset.add_url_rule(
    "/<id>/resource/<resource_id>/download/<filename>", view_func=resource_download
)
ogdch_dataset.add_url_rule(
    "/<id>/resource_permalink/<filename>", view_func=resource_permalink
)
ogdch_dataset.add_url_rule("/<id>/permalink", view_func=dataset_permalink)

ogdch_resource.add_url_rule("/search", view_func=search)
