from flask import Blueprint, make_response
import logging
import mimetypes

import requests
import unicodecsv
from io import StringIO

from ckan.lib.base import render
import ckan.lib.uploader as uploader
import ckan.logic as logic
import ckan.model as model
import paste.fileapp
from ckan.common import _, request, c
import ckan.plugins.toolkit as toolkit
from ckan.lib.plugins import lookup_package_plugin
from ckan.lib.dictization.model_dictize import resource_dictize
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

ogdch = Blueprint('ogdch', __name__)
ogdch_resource = Blueprint('ogdch_resource', __name__)


def email_address_exporter(self):
    if not (c.userobj and c.userobj.sysadmin):
        abort(401, _('Unauthorized'))

    if 'filter' in request.params:
        fobj = StringIO()
        csv = unicodecsv.writer(fobj)
        csv.writerow(['First Name', 'Last Name', 'Email'])

        wp_url = toolkit.config.get('ckanext.switzerland.wp_url')
        api_key = toolkit.config.get('ckanext.switzerland.user_list_api_key')
        url = '{}/wp-admin/admin-post.php?action=user_list&key={}'.format(wp_url, api_key)
        users = requests.get(url).json()['data']

        if request.params['filter'] != 'all':
            followers = get_action('dataset_follower_list')({}, {'id': request.params['filter']})
            followers = {follower['name'] for follower in followers}
            users = filter(lambda u: u['user_login'] in followers, users)

        for user in users:
            csv.writerow([user['first_name'], user['last_name'], user['user_email']])

        response = make_response(fobj.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = 'attachment; filename="emails.csv"'

        return response

    packages = get_action('package_search')({}, {'sort': 'name asc', 'rows': 1000})['results']
    for package in packages:
        package['follower_count'] = get_action('dataset_follower_count')({}, {'id': package['id']})

    c.datasets = packages
    return render('email_exporter/email_exporter.html')


ogdch.add_url_rule('/ckan-admin/email_exporter', view_func=email_address_exporter)


def resource_download(self, id, resource_id, filename=None):
    """
    Provides a direct download by either redirecting the user to the url
    stored or downloading an uploaded file directly.

    Copied from ckan source code, only change is this to allow download of deleted resources:
    - rsc = get_action('resource_show')(context, {'id': resource_id})
    + resource_obj = model.Resource.get(resource_id)
    + rsc = resource_dictize(resource_obj, {'model': model})
    """
    context = {'model': model, 'session': model.Session,
               'user': c.user or c.author, 'auth_user_obj': c.userobj}

    try:
        resource_obj = model.Resource.get(resource_id)
        rsc = resource_dictize(resource_obj, {'model': model})
        get_action('package_show')(context, {'id': id})
    except NotFound:
        abort(404, _('Resource not found'))
    except NotAuthorized:
        abort(401, _('Unauthorized to read resource %s') % id)

    if rsc.get('url_type') == 'upload':
        upload = uploader.ResourceUpload(rsc)
        filepath = upload.get_path(rsc['id'])
        fileapp = paste.fileapp.FileApp(filepath)
        try:
            status, headers, app_iter = request.call_application(fileapp)
        except OSError:
            abort(404, _('Resource data not found'))

        response = make_response(app_iter)
        response.headers.update(dict(headers))
        content_type, content_enc = mimetypes.guess_type(
            rsc.get('url', ''))
        if content_type:
            response.headers['Content-Type'] = content_type
        response.status = status
        return response
    elif not 'url' in rsc:
        abort(404, _('No download is available'))
    redirect(rsc['url'])


def resource_permalink(self, id, filename):
    context = {'model': model, 'session': model.Session,
               'user': c.user or c.author, 'for_view': True,
               'auth_user_obj': c.userobj}
    data_dict = {'id': id, 'include_tracking': True}

    try:
        dataset = get_action('package_show')(context, data_dict)
    except NotFound:
        abort(404, _('Dataset not found'))
    except NotAuthorized:
        abort(401, _('Unauthorized to read package %s') % id)

    for res in dataset['resources']:
        if resource_filename(res['url']) == filename:
            return redirect(res['url'])

    abort(404, _('Resource not found'))


def dataset_permalink(self, id):
    context = {'model': model, 'session': model.Session,
               'user': c.user or c.author, 'for_view': True,
               'auth_user_obj': c.userobj}
    data_dict = {'id': id, 'include_tracking': True}
    try:
        dataset = get_action('package_show')(context, data_dict)
    except NotFound:
        abort(404, _('Dataset not found'))
    except NotAuthorized:
        abort(401, _('Unauthorized to read package %s') % id)

    if not dataset['permalink']:
        abort(404, _('Resource not found'))

    return redirect(dataset['permalink'])


def search(self):
    return render('search/search.html')


ogdch_resource.add_url_rule('/dataset/{id}/resource/{resource_id}/download', resource_download)
ogdch_resource.add_url_rule('/dataset/{id}/resource/{resource_id}/download/{filename}', resource_download)
ogdch_resource.add_url_rule('/dataset/{id}/resource_permalink/{filename}', resource_permalink)
ogdch_resource.add_url_rule('/dataset/{id}/permalink', dataset_permalink)
ogdch_resource('/search', search)
