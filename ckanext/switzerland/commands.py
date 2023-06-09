import itertools
import os
import sys
import traceback
from datetime import datetime

import ckan.logic as logic
import ckan.model as model
from ckan.lib.cli import CkanCommand


class OgdchCommands(CkanCommand):
    """
    Commands for opendata.swiss
    Usage:
        # General usage
        paster --plugin=ckanext-switzerland ogdch <command> -c <path to config file> # noqa

        # Show this help
        paster ogdch help

        # Cleanup datastore
        paster ogdch cleanup_datastore

        # Cleanup harvester jobs and objects:
        # - deletes all the harvest jobs and objects except the latest n
        # - the default number of jobs to keep is 10
        # - the command can be performed with a dryrun option where the
        #   database will remain unchanged
        paster ogdch cleanup_harvestjobs
            [{source_id}] [--keep={n}] [--dryrun]
    """

    summary = __doc__.split("\n")[0]
    usage = __doc__

    def __init__(self, name):
        super(CkanCommand, self).__init__(name)
        self.parser.add_option(
            "--keep",
            action="store",
            type="int",
            dest="nr_of_jobs_to_keep",
            default=10,
            help="The number of latest harvest jobs to keep",
        )
        self.parser.add_option(
            "--dryrun",
            action="store_true",
            dest="dryrun",
            default=False,
            help="dryrun of cleanup harvestjobs and "
            "publish_scheduled_datasets",
        )

    def command(self):
        # load pylons config
        self._load_config()
        options = {
            "cleanup_datastore": self.cleanup_datastore,
            "help": self.help,
            "cleanup_harvestjobs": self.cleanup_harvestjobs,
        }

        cmd = self.args[0]
        options[cmd](*self.args[1:])

    def help(self):
        print(self.__doc__)

    def cleanup_datastore(self):
        user = logic.get_action("get_site_user")({"ignore_auth": True}, {})
        context = {
            "model": model,
            "session": model.Session,
            "user": user["name"],
        }
        try:
            logic.check_access("datastore_delete", context)
            logic.check_access("resource_show", context)
        except logic.NotAuthorized:
            print("User is not authorized to perform this action.")
            sys.exit(1)

        # query datastore to get all resources from the _table_metadata
        resource_id_list = []
        try:
            for offset in itertools.count(start=0, step=100):
                print(
                    "Load metadata records from datastore (offset: %s)" % offset
                )
                record_list, has_next_page = self._get_datastore_table_page(
                    context, offset
                )  # noqa
                resource_id_list.extend(record_list)
                if not has_next_page:
                    break
        except Exception as e:
            print(
                "Error while gathering resources: %s / %s"
                % (str(e), traceback.format_exc())
            )

        # delete the rows of the orphaned datastore tables
        delete_count = 0
        for resource_id in resource_id_list:
            logic.check_access("datastore_delete", context)
            logic.get_action("datastore_delete")(
                context, {"resource_id": resource_id, "force": True}
            )
            print("Table '%s' deleted (not dropped)" % resource_id)
            delete_count += 1

        print("Deleted content of %s tables" % delete_count)

    def _get_datastore_table_page(self, context, offset=0):
        # query datastore to get all resources from the _table_metadata
        result = logic.get_action("datastore_search")(
            context, {"resource_id": "_table_metadata", "offset": offset}
        )

        resource_id_list = []
        for record in result["records"]:
            try:
                # ignore 'alias' records
                if record["alias_of"]:
                    continue

                logic.check_access("resource_show", context)
                logic.get_action("resource_show")(
                    context, {"id": record["name"]}
                )
                print("Resource '%s' found" % record["name"])
            except logic.NotFound:
                resource_id_list.append(record["name"])
                print("Resource '%s' *not* found" % record["name"])
            except logic.NotAuthorized:
                print("User is not authorized to perform this action.")
            except (KeyError, AttributeError) as e:
                print("Error while handling record %s: %s" % (record, str(e)))
                continue

        # are there more records?
        has_next_page = len(result["records"]) > 0

        return (resource_id_list, has_next_page)

    def cleanup_harvestjobs(self, source=None):
        """
        command for the harvester job cleanup
        :argument source: string (optional)
        :argument number_of_jobs_to_keep: int (optional)
        """
        # get source from arguments
        source_id = None
        data_dict = {}
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
            data_dict["harvest_source_id"] = source_id
            print("cleaning up jobs for harvest source {}".format(source_id))
        else:
            print("cleaning up jobs for all harvest sources")

        # get named arguments
        data_dict["number_of_jobs_to_keep"] = self.options.nr_of_jobs_to_keep
        data_dict["dryrun"] = self.options.dryrun

        # set context
        context = {
            "model": model,
            "session": model.Session,
            "ignore_auth": True,
        }
        admin_user = logic.get_action("get_site_user")(context, {})
        context["user"] = admin_user["name"]

        # test authorization
        try:
            logic.check_access("harvest_sources_clear", context, data_dict)
        except logic.NotAuthorized:
            print("User is not authorized to perform this action.")
            sys.exit(1)

        # perform the harvest job cleanup
        result = logic.get_action("ogdch_cleanup_harvestjobs")(
            context, data_dict
        )

        # print the result of the harvest job cleanup
        self._print_clean_harvestjobs_result(result, data_dict)

    def _print_clean_harvestjobs_result(self, result, data_dict):
        print(
            "\nCleaning up jobs for harvest sources:\n{}\nConfiguration:".format(
                37 * "-"
            )
        )
        self._print_configuration(data_dict)
        print("\nResults per source:\n{}".format(19 * "-"))
        for source in result["sources"]:
            if source.id in result["cleanup"].keys():
                self._print_harvest_source(source)
                self._print_cleanup_result_per_source(
                    result["cleanup"][source.id]
                )
            else:
                self._print_harvest_source(source)
                print("Nothing needs to be done for this source")

        if data_dict["dryrun"]:
            print(
                "\nThis has been a dry run: "
                "if you want to perfom these changes"
                " run this again without the option --dryrun!"
            )
        else:
            print(
                "\nThe database has been cleaned from harvester "
                "jobs and harvester objects."
                " See above about what has been done."
            )

    def _print_harvest_source(self, source):
        print("\n           Source id: {0}".format(source.id))
        print("                 url: {0}".format(source.url))
        print("                type: {0}".format(source.type))

    def _print_cleanup_result_per_source(self, cleanup_result):
        print(
            "   nr jobs to delete: {0}".format(
                len(cleanup_result["deleted_jobs"])
            )
        )
        print(
            "nr objects to delete: {0}".format(
                cleanup_result["deleted_nr_objects"]
            )
        )
        print("      jobs to delete:")
        self._print_harvest_jobs(cleanup_result["deleted_jobs"])

    def _print_configuration(self, data_dict):
        for k, v in data_dict.items():
            print("- {}: {}".format(k, v))

    def _print_harvest_jobs(self, jobs):
        header_list = ["id", "created", "status"]
        row_format = "{:<20}|{:<40}|{:<20}|{:<20}"
        print(row_format.format("", *header_list))
        print(
            "{:<20}+{:<40}+{:<20}+{:<20}".format(
                "", "-" * 40, "-" * 20, "-" * 20
            )
        )
        for job in jobs:
            print(
                row_format.format(
                    "",
                    job.id,
                    job.created.strftime("%Y-%m-%d %H:%M:%S"),
                    job.status,
                )
            )
