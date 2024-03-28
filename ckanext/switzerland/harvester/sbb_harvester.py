import ftplib  # for errors only
import logging
import os
import re
import tempfile
import traceback
from datetime import datetime

import voluptuous
from ckan import model
from ckan.lib.helpers import json
from ckan.logic import NotFound
from ckan.model import Session
from ckan.plugins.toolkit import config as ckanconf

from ckanext.harvest.model import HarvestJob, HarvestObject
from ckanext.switzerland.harvester.base_sbb_harvester import (
    BaseSBBHarvester,
    validate_regex,
)
from ckanext.switzerland.harvester.ist_file import ist_file_filter
from ckanext.switzerland.harvester.storage_adapter_factory import StorageAdapterFactory

log = logging.getLogger(__name__)


class SBBHarvester(BaseSBBHarvester):
    harvester_name = "SBB Harvester"

    filters = {"ist_file": ist_file_filter}

    # tested
    def info(self):
        """
        Return basic information about the harvester

        :returns: Dictionary with basic information about the harvester
        :rtype: dict
        """
        return {
            "name": "%sharvest" % self.harvester_name.lower(),
            "title": self.harvester_name,
            "description": "Fetches data from the SBB FTP/S3 AWS Server",
            "form_config_interface": "Text",
        }

    def get_config_validation_schema(self):
        schema = super(SBBHarvester, self).get_config_validation_schema()
        return schema.extend(
            {
                voluptuous.Required("filter_regex", default=".*"): validate_regex,
                voluptuous.Required("ist_file", default=False): bool,
            }
        )

    def gather_stage_impl(self, harvest_job):  # noqa
        """
        Dummy stage that launches the next phase

        :param harvest_job: Harvester job

        :returns: List of HarvestObject ids that are processed in the next stage
                  (fetch_stage)
        :rtype: list
        """
        # TODO: Simplify this method.
        log.info("=====================================================")
        log.info(
            "In %s Harvester gather_stage" % self.harvester_name
        )  # harvest_job.source.url

        # set harvester config
        self.config = self.load_config(harvest_job.source.config)

        modified_dates = {}

        # get a listing of all files in the target directory

        remotefolder = self.get_remote_folder()
        log.info("Getting listing from remotefolder: %s" % remotefolder)

        try:
            with StorageAdapterFactory(ckanconf).get_storage_adapter(
                remotefolder, self.config
            ) as storage:
                filelist = storage.get_remote_filelist()
                log.info("Remote dirlist: %s" % str(filelist))

                filelist = list(
                    [
                        filename
                        for filename in filelist
                        if re.match(self.config["filter_regex"], filename)
                    ]
                )

                # get last-modified date of each file
                for f in filelist:
                    modified_dates[f] = storage.get_modified_date(f)

                # store some config for the next step

                # store retrieved files in a folder, e.g. 'ftp-secure.sbb.ch:990'
                storagelibfolder = storage.get_top_folder()

                # set base directory of the tmp folder
                tmpdirbase = os.path.join(
                    storage.get_local_path(),
                    storagelibfolder.strip("/"),
                    remotefolder.lstrip("/"),
                )
                tempfile.tempdir = tmpdirbase

                # the base tmp folder needs to be created for the tempfile library
                if not os.path.exists(tmpdirbase):
                    storage.create_local_dir(tmpdirbase)

                # set prefix for tmp folder
                prefix = datetime.now().strftime(self.tmpfolder_prefix)
                # save the folder path where the files are to be downloaded
                # all parts following the first one must be relative paths

                workingdir = tempfile.mkdtemp(prefix=prefix)
                log.info("Created workingdir: %s" % workingdir)

        except ftplib.all_errors:
            log.exception("Error getting remote directory listing")
            self._save_gather_error(
                "Error getting remote directory listing: {}".format(
                    traceback.format_exc()
                ),
                harvest_job,
            )
            return None

        if not len(filelist):
            log.warning("No files found in %s", remotefolder)
            return None

        # create one harvest job for each resource in the package
        # -------------------------------------------------------------------------
        object_ids = []

        # ------------------------------------------------------
        # 1: only download the resources that have been modified
        # has there been a previous run and was it successful?
        previous_job = (
            Session.query(HarvestJob)
            .filter(HarvestJob.source == harvest_job.source)
            .filter(HarvestJob.gather_finished.isnot(None))
            .filter(HarvestJob.id != harvest_job.id)
            .order_by(HarvestJob.gather_finished.desc())
            .limit(1)
            .first()
        )
        if (
            previous_job
            and not previous_job.gather_errors
            and previous_job.gather_started
        ):
            # optional 'force_all' config setting can be used to always download all
            # files
            force_all = self.config["force_all"]

            if not force_all:
                try:
                    existing_dataset = self._get_dataset(self.config["dataset"])
                    package = model.Package.get(existing_dataset["id"])
                    # Request only the resources modified since last harvest job
                    for f in filelist[:]:
                        modified_date = modified_dates.get(f)
                        # skip file if it's older than last harvester run date and it
                        # actually exists on the dataset
                        if (
                            modified_date
                            and modified_date < previous_job.gather_started
                        ):
                            # do not run the harvest for this file
                            filelist.remove(f)

                    if not len(filelist):
                        log.info(
                            "No files have been updated on the ftp/s3 aws server "
                            "since the last harvest job"
                        )
                        return []  # no files to harvest this time
                except NotFound:  # dataset does not exist yet, download all files
                    pass
            else:
                log.warning(
                    "force_all is activated, downloading all files from ftp/s3 without "
                    "modification date checking"
                )

            # ------------------------------------------------------

        # ------------------------------------------------------
        # 2: download all resources
        for f in filelist:
            obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
            # serialise and store the dirlist

            data = {
                "type": "file",
                "file": f,
                "workingdir": workingdir,
                "remotefolder": remotefolder,
                "dataset": self.config["dataset"],
            }

            if self.config["ist_file"]:
                data["filter"] = "ist_file"

            obj.content = json.dumps(data)

            # save it for the next step
            obj.save()
            object_ids.append(obj.id)

        # ------------------------------------------------------
        # 3: Add finalizer tasks to queue
        obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
        obj.content = json.dumps({"type": "remove_tempdir", "tempdir": tmpdirbase})
        obj.save()
        object_ids.append(obj.id)

        obj = HarvestObject(guid=self.harvester_name, job=harvest_job)
        obj.content = json.dumps(
            {"type": "finalizer", "dataset": self.config["dataset"]}
        )
        obj.save()
        object_ids.append(obj.id)
        # ------------------------------------------------------
        # send the jobs to the gather queue
        return object_ids
