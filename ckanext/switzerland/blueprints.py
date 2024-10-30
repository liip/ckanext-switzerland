import logging
from typing import Optional, Union

import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.lib.uploader as uploader
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import flask
from ckan.common import _, c, current_user
from ckan.lib import signals
from ckan.lib.dictization.model_dictize import resource_dictize
from ckan.lib.plugins import lookup_package_plugin
from ckan.types import Context, Response
from ckan.views.dataset import search
from flask import Blueprint
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

ogdch_dataset = Blueprint("ogdch_dataset", __name__, url_prefix="/dataset")
ogdch_home = Blueprint("ogdch_home", __name__, url_defaults={"package_type": "dataset"})


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
        resp = flask.send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype=rsc.get("mimetype"),
        )

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

ogdch_home.add_url_rule("/", view_func=search, strict_slashes=False)
