"""
CKAN Base for FTP/S3 Harvester
==================

A Harvesting Job is performed in three phases.
1) the **gather** stage collects all the files that need to be fetched
from the harvest source. Errors occurring in this phase
(``HarvestGatherError``) are stored in the ``harvest_gather_error``
table.
2) the **fetch** stage retrieves the ``HarvestedObjects``
3) the **import** stage stores them in the database. Errors occurring in the second and
third stages (``HarvestObjectError``) are stored in the ``harvest_object_error`` table.
"""

import ftplib  # for errors only
import io
import logging
import mimetypes
import os
import re
import shutil
import time
import traceback
import zipfile
from datetime import datetime

import voluptuous
from ckan import model
from ckan.lib import helpers, search, uploader
from ckan.lib.helpers import json
from ckan.lib.munge import munge_filename, munge_name
from ckan.logic import NotFound, ValidationError, check_access, get_action
from ckan.model import Session
from ckan.plugins.toolkit import config as ckanconf
from simplejson.scanner import JSONDecodeError
from sqlalchemy.sql import bindparam, update
from werkzeug.datastructures import FileStorage

from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.switzerland.harvester.storage_adapter_factory import StorageAdapterFactory
from ckanext.switzerland.helpers import resource_filename

log = logging.getLogger(__name__)


def validate_regex(regex):
    try:
        re.compile(regex)
    except re.error:
        raise voluptuous.Invalid('Invalid regex: "{}"'.format(regex))
    return regex


class BaseSBBHarvester(HarvesterBase):
    """
    A Base SBB Harvester for harvesting data from ftp/s3 aws server.
    This is a generic harvester, which can be configured
    for specific datasets using the ckan harvester webinterface.
    """

    config = None  # ckan harvester config, not ftp/s3 config

    api_version = 2
    action_api_version = 3

    # default remote directory to harvest, to be overwritten by child classes
    # e.g. infodoc or didok
    remotefolder = ""

    # if a resource is uploaded with a format, it will show a tag on the dataset, e.g.
    # XML or TXT the default setting is defined to be TXT for files with no extension
    default_format = "TXT"
    default_mimetype = "text/plain"
    default_mimetype_inner = None

    tmpfolder_prefix = "%d%m%Y-%H%M-"

    # default package metadata
    package_dict_meta = {
        # package privacy
        "private": False,
        "state": "active",
        "isopen": False,
        # --------------------------------------------------------------------------
        # author and maintainer
        "author": "Author name",
        "author_email": "author@example.com",
        "maintainer": "Maintainer name",
        "maintainer_email": "maintainer@example.com",
        # license
        "license_id": "",
        "license_title": "",
        "rights": "",
        # ckan multilang/switzerland custom required fields
        "coverage": "Coverage",
        "spatial": "Spatial",
        "accrual_periodicity": "",
        # --------------------------------------------------------------------------
        "description": {"fr": "", "en": "", "de": "", "it": ""},
        # --------------------------------------------------------------------------
        "groups": [],
        "tags": [],
        "extras": [],
        "language": ["en", "de", "fr", "it"],
        # relations
        "relations": [{}],
        "relationships_as_object": [],
        "relationships_as_subject": [],
        "see_alsos": [],
        "publishers": [{"label": "Publisher 1"}],
        # keywords
        "keywords": {"fr": [], "en": [], "de": [], "it": []},
        "contact_points": [{"name": "Contact Name", "email": "contact@example.com"}],
        "temporals": [
            {"start_date": "2014-03-21T00:00:00", "end_date": "2019-03-21T00:00:00"}
        ],
    }

    resource_dict_meta = {
        "state": "active",
        "rights": "",
        "license": "",
        "coverage": "Coverage",
        "description": {"de": "", "en": "", "fr": "", "it": ""},
    }

    filters = {}

    def _save_gather_error(self, message, job):
        message = message.replace("\n", "<br>")
        log.warning(message)

        return super(BaseSBBHarvester, self)._save_gather_error(
            message=message, job=job
        )

    def _save_object_error(self, message, object, stage="Fetch"):
        message = message.replace("\n", "<br>")

        return super(BaseSBBHarvester, self)._save_object_error(
            message=message, object=object, stage=stage
        )

    def get_remote_folder(self):
        # in the future we want to get directly a path to the folder in the config file
        if self.config.get("environment") is not None:
            return os.path.join("/", self.config["environment"], self.config["folder"])
        else:
            return os.path.join("/", self.config["folder"])

    def validate_config(self, config_str):
        """
        Validates the configuration that can be pasted into the harvester web interface

        :param config_str: Configuration (JSON-encoded string)
        :type config_str: str

        :returns: Configuration dictionary
        :rtype: dict
        """
        if not config_str:
            raise ValueError("Harvester Configuration is required")
        self.load_config(config_str)
        return config_str

    def get_config_validation_schema(self):
        return voluptuous.Schema(
            {
                "environment": str,
                voluptuous.Required("folder"): str,
                voluptuous.Required("dataset"): str,
                voluptuous.Required("resource_regex", default=".*"): validate_regex,
                voluptuous.Required("force_all", default=False): bool,
                "max_resources": int,
                "max_revisions": int,
                "ftp_server": str,
                "storage_adapter": str,
                "bucket": str,
                voluptuous.Required("date_pattern", default=""): str,
            }
        )

    def load_config(self, config_str):
        schema = self.get_config_validation_schema()
        data = json.loads(config_str)
        return schema(data)

    # tested
    def _add_harvester_metadata(self, package_dict):
        """
        Adds the metadata stored in the harvester class

        :param package_dict: Package metadata
        :type package_dict: dict

        :returns: Package dictionary
        :rtype: dict
        """

        # is there a package meta configuration in the harvester?
        if self.package_dict_meta:
            # add each key/value from the meta data of the harvester
            for key, val in list(self.package_dict_meta.items()):
                package_dict[key] = val

        return package_dict

    # tested
    def _add_package_tags(self, package_dict):
        """
        Create tags

        :param package_dict: Package metadata
        :type package_dict: dict

        :returns: Package dictionary
        :rtype: dict
        """
        if "tags" not in package_dict:
            package_dict["tags"] = []

        # add the tags from the config object
        default_tags = self.config.get("default_tags", [])
        if default_tags:
            package_dict["tags"].extend(
                [t for t in default_tags if t not in package_dict["tags"]]
            )

        package_dict["num_tags"] = len(package_dict["tags"])

        return package_dict

    # tested
    def _add_package_groups(self, package_dict, context):
        """
        Create (default) groups

        :param package_dict: Package metadata
        :type package_dict: dict
        :param context: CKAN context
        :type context: dict

        :returns: Package dictionary
        :rtype: dict
        """
        if "groups" not in package_dict:
            package_dict["groups"] = []

        # Set default groups if needed
        default_groups = self.config.get("default_groups", [])

        # one might also enter just a string -> convert it to list
        if not isinstance(default_groups, list):
            default_groups = [default_groups]

        # check if groups exist locally, otherwise do not add them
        for group_name in default_groups:
            try:
                group = get_action("group_show")(context, {"id": group_name})
                if self.api_version == 1:
                    package_dict["groups"].append(group["name"])
                else:
                    package_dict["groups"].append(group["id"])
            except NotFound:
                log.info("Group %s is not available" % group_name)

        return package_dict

    def _add_package_orgs(self, package_dict, context, organization):
        """
        Fetch organization and set it on the package_dict

        :param package_dict: Package metadata
        :type package_dict: dict
        :param context: CKAN context
        :type context: dict

        :returns: Package dictionary
        :rtype: dict
        """

        # check if this organization exists
        org_dict = get_action("organization_show")(context, {"id": organization})
        if org_dict:
            package_dict["owner_org"] = organization
            package_dict["organization"] = org_dict

        return package_dict

    # tested
    def _add_package_extras(self, package_dict, harvest_object):
        """
        Create default organization(s)

        :param package_dict: Package metadata
        :type package_dict: dict
        :param harvest_object: Instance of the Harvester Object

        :returns: Package dictionary
        :rtype: dict
        """

        # Set default extras if needed
        default_extras = self.config.get("default_extras", {})
        if default_extras:
            override_extras = self.config.get("override_extras", False)
            if "extras" not in package_dict:
                package_dict["extras"] = {}
            for key, value in list(default_extras.items()):
                if key not in package_dict["extras"] or override_extras:
                    # Look for replacement strings
                    if isinstance(value, str):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip("/"),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict["id"],
                        )

                    package_dict["extras"][key] = value

        return package_dict

    # tested
    def remove_tmpfolder(self, tmpfolder):
        """Remove the tmp folder, if it exists"""
        try:
            shutil.rmtree(tmpfolder)
        except OSError:
            pass

    # tested
    def cleanup_after_error(self, retobj):
        """Do some clean-up tasks"""
        if retobj and "tmpfolder" in retobj:
            self.remove_tmpfolder(retobj["tmpfolder"])

    # tested
    def find_resource_in_package(self, dataset, filepath):
        """
        Identify a resource in a package by its (munged) filename

        :param dataset: dataset dictionary
        :type dataset: dict
        :param filepath: Local path to the downloaded file
        :type filepath: str

        :returns: Package dictionary
        :rtype: dict
        """
        resource_meta = {}
        if "resources" in dataset and len(dataset["resources"]):
            # Find resource in the existing packages resource list
            for res in dataset["resources"]:
                # match the resource by its filename
                match_name = munge_filename(os.path.basename(filepath))
                if os.path.basename(res.get("url")) != match_name:
                    continue
                resource_meta = res
                # there should only be one file with the same name in each dataset, so
                # we can break
                break
        return resource_meta

    def _get_dataset(self, dataset):
        return get_action("ogdch_dataset_by_identifier")({}, {"identifier": dataset})

    def _get_mimetypes(self, filename):
        resource_formats = helpers.resource_formats()
        guess, encoding = mimetypes.guess_type(filename, strict=False)

        if guess is None or resource_formats.get(guess.lower()) is None:
            log.info(
                f"Couldn't get a valid resource format from the filename {filename}"
            )
            return (
                self.default_format,
                self.default_mimetype,
                self.default_mimetype_inner,
            )

        # format_info is a list, containing the following values:
        # [
        #     canonical mimetype lowercased,
        #     canonical format (uppercase),
        #     human readable form
        # ]
        format_info = resource_formats.get(guess.lower())

        file_format = format_info[1]
        mimetype = format_info[0]
        mimetype_inner = None

        if format_info[0] == "application/zip":
            try:
                zip_file = zipfile.ZipFile(filename)
            except zipfile.BadZipFile:
                log.warning(f"The file {filename} is not a valid zip file")
                return file_format, mimetype, mimetype_inner

            namelist = zip_file.namelist()

            for name in namelist:
                guess, encoding = mimetypes.guess_type(name, strict=False)
                if guess is not None and resource_formats.get(guess.lower()):
                    # We can only save one value to mimetype_inner, so once we get a
                    # valid mimetype, we can stop looking
                    mimetype_inner = guess
                    break

        return file_format, mimetype, mimetype_inner

    # =======================================================================
    # GATHER Stage
    # =======================================================================

    def gather_stage(self, harvest_job):
        try:
            return self.gather_stage_impl(harvest_job)
        except Exception:
            log.exception("Gather stage failed")
            self._save_gather_error(
                "Gather stage failed: {}".format(traceback.format_exc()), harvest_job
            )
            return []

    def gather_stage_impl(self, harvest_job):
        raise NotImplementedError

    # =======================================================================
    # FETCH Stage
    # =======================================================================

    def fetch_stage(self, harvest_object):
        try:
            return self._fetch_stage(harvest_object)
        except Exception:
            log.exception("Fetch stage failed")
            self._save_object_error(
                "Fetch stage failed: {}".format(traceback.format_exc()),
                harvest_object,
                "Fetch",
            )
            return False

    def _fetch_stage(self, harvest_object):  # noqa
        """
        Fetching of resources. Runs once for each gathered resource.

        :param harvest_object: HarvesterObject instance

        :returns: Whether HarvestObject was saved or not
        :rtype: mixed
        """
        # TODO: Simplify this method.
        log.info("=====================================================")
        log.info(
            "In %s fetch_stage from source %s",
            self.harvester_name,
            harvest_object.job.source.title,
        )
        stage = "Fetch"

        if not harvest_object or not harvest_object.content:
            log.error("No harvest object received")
            self._save_object_error("No harvest object received", harvest_object, stage)
            return False

        try:
            obj = json.loads(harvest_object.content)
        except JSONDecodeError as e:
            log.error("Invalid harvest object: %s", harvest_object.content)
            self._save_object_error(
                "Unable to decode harvester info: %s" % str(e),
                harvest_object.content,
                stage,
            )
            return False

        log.info("Harvest object json: %s", harvest_object.content)

        if obj["type"] != "file":
            return True

        # the file to harvest from the previous step
        f = obj.get("file")
        if not f:
            self._save_object_error(
                "No file to harvest: %s" % harvest_object.content, harvest_object, stage
            )
            return False

        # the folder where the file is to be downloaded to
        if "workingdir" not in obj and "tmpfolder" in obj:
            # This happens sometimes for the S3 harvesters, and I have been unable to
            # find the reason for it. There is no need to save an error here, as it
            # doesn't prevent any resource from being harvested, but we should log it.
            log.info(
                "Fetch stage received a harvest object that has already been "
                "processed by this stage: %s" % harvest_object.__dict__,
            )
            # returning "unchanged" so as not to save an error or continue to import
            # stage
            return "unchanged"
        tmpfolder = obj.get("workingdir")
        if not tmpfolder:
            self._save_object_error(
                "No tmpfolder received from gather step: %s" % harvest_object.content,
                harvest_object,
                stage,
            )
            return False

        remotefolder = obj.get("remotefolder")
        if not remotefolder:
            self._save_object_error(
                "No remotefolder received from gather step: %s"
                % harvest_object.content,
                harvest_object,
                stage,
            )
            return False

        log.info("Remote directory: %s", remotefolder)
        log.info("Local directory: %s", tmpfolder)

        self.config = self.load_config(harvest_object.job.source.config)

        try:
            with StorageAdapterFactory(ckanconf).get_storage_adapter(
                remotefolder, self.config
            ) as storage:
                # fetching file
                # -------------------------------------------------------------------
                # full path of the destination file
                targetfile = os.path.join(tmpfolder, f)

                log.info("Fetching file: %s" % str(f))

                start = time.time()
                status = storage.fetch(f, targetfile)  # 226 Transfer complete
                elapsed = time.time() - start

                log.info("Fetched %s [%s] in %ds" % (f, str(status), elapsed))

                if "226" not in status:
                    self._save_object_error(
                        "Download error for file %s: %s" % (f, str(status)),
                        harvest_object,
                        stage,
                    )
                    return False

        except ftplib.all_errors:
            log.exception("Ftplib error")
            self._save_object_error(
                "Ftplib error: {}".format(traceback.format_exc()), harvest_object, stage
            )
            self.cleanup_after_error(tmpfolder)
            return False

        except Exception:
            log.exception("An error occurred")
            self._save_object_error(
                "An error occurred: {}".format(traceback.format_exc()),
                harvest_object,
                stage,
            )
            self.cleanup_after_error(tmpfolder)
            return False

        # store the info for the next step
        retobj = {
            "type": "file",
            "file": targetfile,
            "tmpfolder": tmpfolder,
            "dataset": obj["dataset"],
        }
        if "filter" in obj:
            retobj["filter"] = obj["filter"]

        # Save the directory listing and other info in the HarvestObject
        # serialise the dictionary
        harvest_object.content = json.dumps(retobj)
        harvest_object.save()
        return True

    # =======================================================================
    # IMPORT Stage
    # =======================================================================

    def import_stage(self, harvest_object):
        try:
            return self._import_stage(harvest_object)
        except Exception:
            log.exception("Import stage failed")
            self._save_object_error(
                "Import stage failed: {}".format(traceback.format_exc()),
                harvest_object,
                "Import",
            )
            return False

    def _import_stage(self, harvest_object):  # noqa
        """
        Importing the fetched files into CKAN storage.
        Runs once for each fetched resource.

        :param harvest_object: HarvesterObject instance

        :returns: True if the create or update occurred ok,
                  'unchanged' if it didn't need updating or
                  False if there were errors.
        :rtype: bool|string
        """
        # TODO: Simplify this method.
        log.info("=====================================================")
        log.info(
            "In %s import_stage from source %s",
            self.harvester_name,
            harvest_object.job.source.title,
        )

        stage = "Import"

        if not harvest_object or not harvest_object.content:
            log.error("No harvest object received: %s" % harvest_object)
            self._save_object_error(
                "Empty content for harvest object %s" % harvest_object.id,
                harvest_object,
                stage,
            )
            return False

        try:
            obj = json.loads(harvest_object.content)
        except JSONDecodeError as e:
            log.error("Invalid harvest object: %s", harvest_object)
            self._save_object_error(
                "Unable to decode harvester info: %s" % str(e), harvest_object, stage
            )
            return False

        log.info("Harvest object json: %s", harvest_object.content)

        # set harvester config
        self.config = self.load_config(harvest_object.job.source.config)

        if obj["type"] == "finalizer":
            self.finalize(harvest_object, obj)
            return True

        if obj["type"] == "remove_tempdir":
            self.remove_tmpfolder(obj["tempdir"])
            return True

        if "filter" in obj:
            file_filter = self.filters[obj["filter"]]
            obj = file_filter(obj, self.config)

        filepath = obj.get("file")
        if not filepath:
            log.error("Invalid file key in harvest object: %s" % obj)
            self._save_object_error("No file to import", harvest_object, stage)
            return False

        tmpfolder = obj.get("tmpfolder")
        if not tmpfolder:
            log.error("invalid tmpfolder in harvest object: %s" % obj)
            self._save_object_error(
                "Could not get path of temporary folder: %s" % tmpfolder,
                harvest_object,
                stage,
            )
            return False

        context = {"model": model, "session": Session, "user": self._get_user_name()}

        now = datetime.utcnow().isoformat()

        # =======================================================================
        # package
        # =======================================================================

        old_resource_id = None
        old_resource_meta = {}

        try:
            # -----------------------------------------------------------------------
            # use the existing package dictionary (if it exists)
            # -----------------------------------------------------------------------

            dataset = self._get_dataset(obj["dataset"])
            log.info("Using existing package with id %s", str(dataset.get("id")))

            # update version of package
            dataset["version"] = now

            # check if there is a resource matching the filename in the package
            old_resource_meta = self.find_resource_in_package(dataset, filepath)
            if old_resource_meta:
                log.info(
                    "Found existing resource with this filename: %s"
                    % str(old_resource_meta)
                )
                old_resource_id = old_resource_meta["id"]

        except NotFound:
            # -----------------------------------------------------------------------
            # create the package dictionary instead
            # -----------------------------------------------------------------------

            # add the metadata from the harvester

            package_dict = {
                "name": munge_name(obj["dataset"]),
                "identifier": obj["dataset"],
            }

            package_dict = self._add_harvester_metadata(package_dict)

            # title of the package
            if "title" not in package_dict:
                package_dict["title"] = {
                    "de": obj["dataset"],
                    "en": obj["dataset"],
                    "fr": obj["dataset"],
                    "it": obj["dataset"],
                }
            # for DCAT schema - same info as in the title
            if "display_name" not in package_dict:
                package_dict["display_name"] = package_dict["title"]

            package_dict["creator_user_id"] = model.User.get(context["user"]).id

            # fill with empty defaults
            for key in ["issued", "metadata_created"]:
                package_dict[key] = now
            for key in [
                "resources",
                "groups",
                "tags",
                "extras",
                "contact_points",
                "relations",
                "relationships_as_object",
                "relationships_as_subject",
                "publishers",
                "see_alsos",
                "temporals",
            ]:
                if key not in package_dict:
                    package_dict[key] = []
            for key in ["keywords"]:
                if key not in package_dict:
                    package_dict[key] = {}

            package_dict["source_type"] = self.info()["name"]

            # count keywords or tags
            package_dict["num_tags"] = 0
            tags = (
                package_dict.get("keywords")
                if package_dict.get("keywords", {})
                else package_dict.get("tags", {})
            )
            # count the english tags (if available)
            if tags and "en" in tags and isinstance(tags["en"], list):
                package_dict["num_tags"] = len(tags["en"])

            if "language" not in package_dict:
                package_dict["language"] = ["en", "de", "fr", "it"]

            package_dict = self._add_package_tags(package_dict)
            package_dict = self._add_package_groups(package_dict, context)
            source_org = model.Package.get(harvest_object.source.id).owner_org
            package_dict = self._add_package_orgs(package_dict, context, source_org)
            package_dict = self._add_package_extras(package_dict, harvest_object)

            # version
            package_dict["version"] = now

            # -----------------------------------------------------------------------
            # create the package
            # -----------------------------------------------------------------------

            log.debug("Package dict (pre-creation): %s" % str(package_dict))

            # This logic action requires to call check_access to prevent the Exception:
            # 'Action function package_show  did not call its auth function'
            # Adds action name onto the __auth_audit stack
            if not check_access("package_create", context):
                self._save_object_error(
                    "%s not authorised to create packages (object %s)"
                    % (self.harvester_name, harvest_object.id),
                    harvest_object,
                    stage,
                )
                return False

            try:
                # create the dataset
                dataset = get_action("package_create")(context, package_dict)
            except ValidationError as e:
                self._save_object_error(
                    f"Validation error creating dataset: {e}",
                    harvest_object,
                    stage,
                )
                return False

            log.info("Created package: %s" % str(dataset["name"]))

        except Exception:
            log.exception("Package update/creation error")
            self._save_object_error(
                "Package update/creation error: {}".format(traceback.format_exc()),
                harvest_object,
                stage,
            )
            return False

        # need a dataset to continue
        if not dataset:
            self._save_object_error(
                "Could not update or create package: %s" % self.harvester_name,
                harvest_object,
                stage,
            )
            return False

        # =======================================================================
        # resource
        # =======================================================================

        log.info("Importing file: %s" % str(filepath))

        site_url = ckanconf.get("ckan.site_url", None)
        if not site_url:
            self._save_object_error(
                "Could not get site_url from CKAN config file", harvest_object, stage
            )
            return False

        log.info("Adding %s to package with id %s", str(filepath), dataset["id"])

        fp = None
        try:
            try:
                size = int(os.path.getsize(filepath))
            except ValueError:
                size = None

            fp = open(filepath, "rb")

            file_name = os.path.basename(filepath)

            # -----------------------------------------------------
            # create new resource
            # -----------------------------------------------------

            if not old_resource_meta:
                # There is no existing resource for this filename, but we still want to
                # find an old resource to copy some metadata from, if one exists.
                old_resources, _ = self._get_ordered_resources(dataset)
                if len(old_resources):
                    old_resource_meta = old_resources[0]
                    log.info(
                        "Using existing resource to copy metadata from: %s"
                        % str(old_resource_meta)
                    )

            resource_meta = self.resource_dict_meta

            resource_meta["identifier"] = file_name

            file_format, mimetype, mimetype_inner = self._get_mimetypes(filepath)
            resource_meta["format"] = file_format
            resource_meta["media_type"] = mimetype
            resource_meta["mimetype"] = mimetype
            resource_meta["mimetype_inner"] = mimetype_inner

            resource_meta["name"] = {
                "de": file_name,
                "en": file_name,
                "fr": file_name,
                "it": file_name,
            }
            resource_meta["title"] = {
                "de": file_name,
                "en": file_name,
                "fr": file_name,
                "it": file_name,
            }

            resource_meta["issued"] = now
            resource_meta["version"] = now

            # take this metadata from the old version if available
            fields_from_old_resource_meta = [
                "rights",
                "license",
                "coverage",
                "description",
                "relations",
            ]
            for field in fields_from_old_resource_meta:
                if old_resource_meta.get(field):
                    resource_meta[field] = old_resource_meta.get(field)

            resource_meta["package_id"] = dataset["id"]

            # url parameter is ignored for resource uploads, but required by ckan
            # this parameter will be replaced later by the resource patch with a link to
            # the download file
            resource_meta["url"] = "http://dummy-value"
            resource_meta["download_url"] = None

            if size is not None:
                resource_meta["size"] = size
                resource_meta["byte_size"] = size

            log.info("Creating new resource: %s" % str(resource_meta))

            with open(filepath, "rb") as f:
                stream = io.BytesIO(f.read())

            upload = FileStorage(stream=stream, filename=file_name)

            resource_meta["upload"] = upload
            resource_meta["modified"] = now

            created_resource = get_action("resource_create")(context, resource_meta)
            log.info("Successfully created resource {}".format(created_resource["id"]))

            # delete the old version of the resource
            if old_resource_id:
                log.info("Deleting old resource: %s", old_resource_id)

                # delete the datastore table
                try:
                    get_action("datastore_delete")(
                        context, {"resource_id": old_resource_id, "force": True}
                    )
                except NotFound:
                    pass  # Sometimes importing the data into the datastore fails

                try:
                    get_action("resource_delete")(context, {"id": old_resource_id})
                except NotFound:
                    self._save_object_error(
                        f"Error deleting old resource {old_resource_id} for "
                        f"filename {file_name}: the resource was not found. "
                        f"This could be due to a failed connection to the database. "
                        f"{traceback.format_exc()}",
                        harvest_object,
                        stage,
                    )

            Session.commit()

            log.info("Successfully harvested file %s" % filepath)

            # ---------------------------------------------------------------------

        except Exception:
            log.exception("Error adding resource")
            self._save_object_error(
                "Error adding resource: {}".format(traceback.format_exc()),
                harvest_object,
                stage,
            )
            return False

        finally:
            # close the file pointer
            if fp:
                fp.close()
        return True

    def _get_ordered_resources(self, package):
        ordered_resources = []
        unmatched_resources = []

        # get filename regex for permalink from harvester config or fallback to a
        # catch-all
        identifier_regex = self.config["resource_regex"]
        for resource in package["resources"]:
            if re.match(identifier_regex, resource["identifier"], re.IGNORECASE):
                ordered_resources.append(resource)
            else:
                unmatched_resources.append(resource)

        if self.config["date_pattern"]:
            ordered_resources.sort(
                key=lambda r: re.search(
                    self.config["date_pattern"], r["identifier"]
                ).group(),
                reverse=True,
            )
        else:
            ordered_resources.sort(key=lambda r: r["identifier"], reverse=True)

        return ordered_resources, unmatched_resources

    def finalize(self, harvest_object, harvest_object_data):
        context = {"model": model, "session": Session, "user": self._get_user_name()}
        stage = "Import"

        log.info("Running finalizing tasks:")
        # ----------------------------------------------------------------------------
        # Deleting old resources, generate permalink, order resources:
        # We do this by matching a regex, defined in the `resource_regex` key of the
        # harvester json config, against the identifier (filename) of the resources of
        # the dataset. The ones that matched are thrown in a list and sorted by name,
        # descending. This makes the newest file appear first when the filesnames have
        # the correct format (YYYY-MM-DD-*).
        # In case filesnames have different structure, e.g., *_YYYY-MM-DD.csv,
        # `date_pattern` should be specified in the harvester configuration, which is
        # used to list the newest files on the top of the list.
        # The oldest files of this list get deleted if there are more than
        # harvester_config.max_resources in the list.
        # The newest file is set as a permalink on the dataset.
        # The sorted list of resources get set on the dataset, with not matched
        # resources appearing first.

        # ----------------------------------------------------------------------------
        # reorder resources
        try:
            package = self._get_dataset(harvest_object_data["dataset"])
        except NotFound:
            message = (
                f"Dataset {harvest_object_data['dataset']} was not found, "
                f"so not carrying out finalizing tasks. Check other errors for the "
                f"reason we could not save this dataset."
            )
            log.exception(message)
            self._save_object_error(message, harvest_object, "Import")

            return True

        try:
            ordered_resources, unmatched_resources = self._get_ordered_resources(
                package
            )
        except NotFound:
            self._save_object_error(
                f"Error reordering resources for dataset "
                f"{harvest_object_data['dataset']}. "
                f"This could be due to a failed connection to the database. "
                f"{traceback.format_exc()}",
                harvest_object,
                stage,
            )

            return False

        # ----------------------------------------------------------------------------
        # delete old resources
        max_resources = self.config.get("max_resources")
        resources_count = len(ordered_resources)

        if max_resources and resources_count > max_resources:
            log.info(
                "Found %s Resources, max resources is %s, deleting %s resources",
                resources_count,
                max_resources,
                resources_count - max_resources,
            )

            for resource in ordered_resources[max_resources:]:
                try:
                    self._delete_version(
                        context, package, resource_filename(resource["url"])
                    )
                except Exception as e:
                    self._save_object_error(
                        "Error deleting resource {} in finalizing tasks: {}".format(
                            resource["id"], e
                        ),
                        harvest_object,
                        stage,
                    )

            ordered_resources = ordered_resources[:max_resources]

        # set permalink on dataset
        if ordered_resources:
            permalink = ordered_resources[0]["url"]
            log.info("Permalink for dataset %s is %s", package["name"], permalink)
        else:
            permalink = None

        now = datetime.utcnow().isoformat()
        get_action("package_patch")(
            context,
            {
                "id": package["id"],
                "permalink": permalink,
                "modified": now,
                "metadata_modified": now,
            },
        )

        # reorder resources
        # not matched resources come first in the list, then the ordered
        get_action("package_resource_reorder")(
            context,
            {
                "id": package["id"],
                "order": [r["id"] for r in unmatched_resources + ordered_resources],
            },
        )

        from ckanext.harvest.model import harvest_object_table

        conn = Session.connection()
        u = (
            update(harvest_object_table)
            .where(harvest_object_table.c.package_id == bindparam("b_package_id"))
            .values(current=False)
        )
        conn.execute(u, b_package_id=package["id"])

        harvest_object.package_id = package["id"]
        harvest_object.current = True
        harvest_object.save()

        # Defer constraints and flush so the dataset can be indexed with
        # the harvest object id (on the after_show hook from the harvester
        # plugin)
        harvest_object.add()

        model.Session.execute("SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED")
        model.Session.flush()

        search.rebuild(package["id"])

    def _delete_version(self, context, package, filename):
        """Fully delete the resource with the given filename"""
        for resource in package["resources"]:
            if resource_filename(resource["url"]) == filename:
                log.debug(
                    "Deleting resource {} with filename {}".format(
                        resource["id"], filename
                    )
                )
                # delete the file from the filestore
                path = uploader.ResourceUpload(resource).get_path(resource["id"])
                if os.path.exists(path):
                    os.remove(path)

                # delete the datastore table
                try:
                    get_action("datastore_delete")(
                        context, {"resource_id": resource["id"], "force": True}
                    )
                except NotFound:
                    pass  # Sometimes importing the data into the datastore fails

                # delete the resource itself
                get_action("resource_delete")(context, {"id": resource["id"]})
