# coding: utf-8

import itertools
import logging
from collections import OrderedDict

import ckan.plugins.toolkit as tk
from ckan.logic import NotFound, ValidationError
from ckan.plugins.toolkit import get_or_bust, side_effect_free

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestSource
from ckanext.switzerland.helpers import get_content_headers

log = logging.getLogger(__name__)


@side_effect_free
def ogdch_dataset_count(context, data_dict):
    """
    Return the total number of datasets and the number of dataset per group.
    """
    user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    req_context = {"user": user["name"]}

    # group_list contains the number of datasets in the 'packages' field
    groups = tk.get_action("group_list")(req_context, {"all_fields": True})
    group_count = OrderedDict()
    for group in groups:
        group_count[group["name"]] = group["package_count"]

    # get the total number of dataset from package_search
    search_result = tk.get_action("package_search")(
        req_context, {"rows": 0, "fq": "+dataset_type:dataset"}
    )

    return {
        "total_count": search_result["count"],
        "groups": group_count,
    }


@side_effect_free
def ogdch_content_headers(context, data_dict):
    """
    Returns some headers of a remote resource
    """
    url = get_or_bust(data_dict, "url")
    response = get_content_headers(url)
    return {
        "status_code": response.status_code,
        "content-length": response.headers.get("content-length", ""),
        "content-type": response.headers.get("content-type", ""),
    }


@side_effect_free
def ogdch_dataset_terms_of_use(context, data_dict):
    """
    Returns the terms of use for the requested dataset.

    By definition the terms of use of a dataset corresponds
    to the least open rights statement of all distributions of
    the dataset
    """
    terms = [
        "NonCommercialAllowed-CommercialAllowed-ReferenceNotRequired",
        "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
        "NonCommercialAllowed-CommercialWithPermission-ReferenceNotRequired",
        "NonCommercialAllowed-CommercialWithPermission-ReferenceRequired",
        "ClosedData",
    ]
    user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    req_context = {"user": user["name"]}
    pkg_id = get_or_bust(data_dict, "id")
    pkg = tk.get_action("package_show")(req_context, {"id": pkg_id})

    least_open = None
    for res in pkg["resources"]:
        if res["rights"] not in terms:
            least_open = "ClosedData"
            break
        if least_open is None:
            least_open = res["rights"]
            continue
        if terms.index(res["rights"]) > terms.index(least_open):
            least_open = res["rights"]

    if least_open is None:
        least_open = "ClosedData"

    return {"dataset_rights": least_open}


@side_effect_free
def ogdch_dataset_by_identifier(context, data_dict):
    user = tk.get_action("get_site_user")({"ignore_auth": True}, {})
    context.update({"user": user["name"]})
    identifier = get_or_bust(data_dict, "identifier")

    param = 'identifier:"%s"' % identifier
    result = tk.get_action("package_search")(context, {"fq": param})
    try:
        return result["results"][0]
    except (KeyError, IndexError, TypeError):
        raise NotFound


def ogdch_cleanup_harvestjobs(context, data_dict):
    """Cleans up the database for harvest objects and related tables for all
    harvesting jobs except the latest.
    'ckanext.switzerland.number_harvest_jobs_per_source' is the corresponding
    configuration parameter for how many jobs to keep per source.
    The command can be called with or without a source. In the latter case all
    sources are cleaned.
    """

    # check access rights
    tk.check_access("harvest_sources_clear", context, data_dict)
    model = context["model"]
    log.error("checked access rights")

    # get sources from data_dict
    if "harvest_source_id" in data_dict:
        harvest_source_id = data_dict["harvest_source_id"]
        source = HarvestSource.get(harvest_source_id)
        if not source:
            log.error("Harvest source {} does not exist".format(harvest_source_id))
            raise NotFound("Harvest source {} does not exist".format(harvest_source_id))
        sources_to_cleanup = [source]
    else:
        sources_to_cleanup = model.Session.query(HarvestSource).all()

    log.error("got sources:")
    log.error([s.id for s in sources_to_cleanup])

    # get number of jobs to keep form data_dict
    if "number_of_jobs_to_keep" in data_dict:
        number_of_jobs_to_keep = data_dict["number_of_jobs_to_keep"]
    else:
        log.error("Configuration missing for number of harvest jobs to keep")
        raise ValidationError(
            "Configuration missing for number of harvest jobs to keep"
        )

    dryrun = data_dict.get("dryrun", False)

    log.info(
        "Harvest job cleanup called for sources: {},"
        "configuration: {}".format(
            ", ".join([s.id for s in sources_to_cleanup]), data_dict
        )
    )

    # store cleanup result
    cleanup_result = {}
    for source in sources_to_cleanup:
        # get jobs ordered by their creations date
        delete_jobs = (
            model.Session.query(HarvestJob)
            .filter(HarvestJob.source_id == source.id)
            .filter(HarvestJob.status == "Finished")
            .order_by(HarvestJob.created.desc())
            .all()[number_of_jobs_to_keep:]
        )

        # decide which jobs to keep or delete on their order
        delete_jobs_ids = [job.id for job in delete_jobs]

        if not delete_jobs:
            log.debug(
                "Cleanup harvest jobs for source {}: nothing to do".format(source.id)
            )
        else:
            # log all job for a source with the decision to delete or keep them
            log.debug(
                "Cleanup harvest jobs for source {}: delete jobs: {}".format(
                    source.id, delete_jobs_ids
                )
            )

            # get harvest objects for harvest jobs
            delete_objects_ids = (
                model.Session.query(HarvestObject.id)
                .filter(HarvestObject.harvest_job_id.in_(delete_jobs_ids))
                .all()
            )
            delete_objects_ids = list(itertools.chain(*delete_objects_ids))

            # log all objects to delete
            log.debug(
                "Cleanup harvest objects for source {}: delete {} objects".format(
                    source.id, len(delete_objects_ids)
                )
            )

            # perform delete
            sql = """begin;
            delete from harvest_object_error
            where harvest_object_id in ('{delete_objects_values}');
            delete from harvest_object_extra
            where harvest_object_id in ('{delete_objects_values}');
            delete from harvest_object
            where id in ('{delete_objects_values}');
            delete from harvest_gather_error
            where harvest_job_id in ('{delete_jobs_values}');
            delete from harvest_job
            where id in ('{delete_jobs_values}');
            commit;
            """.format(
                delete_objects_values="','".join(delete_objects_ids),
                delete_jobs_values="','".join(delete_jobs_ids),
            )

            # only execute the sql if it is not a dry run
            if not dryrun:
                model.Session.execute(sql)

                # reindex after deletions
                tk.get_action("harvest_source_reindex")(context, {"id": source.id})

            # fill result
            cleanup_result[source.id] = {
                "deleted_jobs": delete_jobs,
                "deleted_nr_objects": len(delete_objects_ids),
            }

            log.info(
                "cleaned resource and shacl result directories {}".format(source.id)
            )

    # return result of action
    return {"sources": sources_to_cleanup, "cleanup": cleanup_result}
