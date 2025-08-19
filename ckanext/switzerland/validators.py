import datetime
import json
import logging
import re
from collections import defaultdict

import ckan.lib.navl.dictization_functions as df
from ckan.common import asbool
from ckan.lib.munge import munge_tag
from ckan.logic import NotFound, get_action
from ckan.model import PACKAGE_NAME_MAX_LENGTH
from ckan.plugins.toolkit import _, missing

from ckanext.scheming.validation import scheming_validator
from ckanext.switzerland.helpers import parse_json

log = logging.getLogger(__name__)
name_match = re.compile(r"[a-z0-9_\-]*$")
user_name_match = re.compile(r"[a-zA-Z0-9_\-@ ]*$")


@scheming_validator
def json_list_of_dicts_field(field, schema):  # noqa C901
    # TODO: Simplify this method.
    field_type = {
        "temporals": {
            "fields": {
                "start_date": lambda date: datetime.datetime.strptime(
                    date, "%d.%m.%Y"
                ).isoformat(),
                "end_date": lambda date: datetime.datetime.strptime(
                    date, "%d.%m.%Y"
                ).isoformat(),
            },
            "required": False,
        },
        "contact_points": {
            "fields": {
                "email": lambda text: text,
                "name": lambda text: text,
            },
            "required": True,
        },
        "publishers": {
            "fields": {
                "label": lambda text: text,
            },
            "required": True,
        },
        "relations": {
            "fields": {
                "url": lambda text: text,
                "label": lambda text: text,
            },
            "required": True,
        },
    }[field["field_name"]]

    def validator(key, data, errors, context):
        # code idea based on ckanext-fluents fluent_text validator

        value = data[key]

        # 1 or 2. dict or JSON encoded string
        if value is not missing:
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except ValueError:
                    errors[key].append(_("Failed to decode JSON string"))
                    return
                except UnicodeDecodeError:
                    errors[key].append(_("Invalid encoding for JSON string"))
                    return
            if not isinstance(value, list):
                errors[key].append(_("expecting JSON list"))
                return

            if not errors[key]:
                data[key] = json.dumps(value)
            return

        """
        3. separate fields/parse form data
        we change the name attribute in the template, so the value is "missing"
        we get the actual values from the __extras dict

        """
        prefix = key[-1] + "-"
        extras = data.get(key[:-1] + ("__extras",), {})

        values = defaultdict(lambda: {})

        # iterate over all extra fields and find our fields
        for name, text in list(extras.items()):
            if not name.startswith(prefix):
                continue

            try:
                # field name example: temporals-1-start_date
                counter, json_field_name = name.split("-")[1:]
                counter = int(counter)
                if json_field_name not in list(field_type["fields"].keys()):
                    raise ValueError
            except ValueError:
                errors[key].append(_("Invalid form data"))
                continue

            if not text:
                if field_type["required"]:
                    errors[(name,)] = [_("This field is required")]
                continue

            try:
                # convert field value
                values[counter][json_field_name] = field_type["fields"][
                    json_field_name
                ](text)
            except ValueError:
                errors[(name,)] = [_("Invalid date")]

        for counter, value in list(values.items()):
            fields = set(field_type["fields"].keys())
            set_fields = set(value.keys())
            missing_fields = fields - set_fields
            for missing_field in missing_fields:
                errors[("{}{}-{}".format(prefix, counter, missing_field),)] = [
                    _("This field is required")
                ]

        # iterate over junk fields (used in resource creation/update)
        junk = data.get(("__junk",))
        if junk:
            for (field_name, counter, json_field_name), value in list(junk.items()):
                if field_name == key[-1]:
                    values[counter][json_field_name] = value

            for counter, field_values in list(values.items()):
                for json_field_name in list(field_values.keys()):
                    del junk[(key[-1], counter, json_field_name)]

        data[key] = json.dumps(list(values.values()))

    return validator


@scheming_validator
def multiple_text(field, schema):
    """
    Accept zero or more values as a list and convert
    to a json list for storage:
    1. a list of strings, eg.:
       ["somevalue a", "somevalue -b"]
    2. a single string for single item selection in form submissions:
       "somevalue-a"
    """

    def validator(key, data, errors, context):
        # if there was an error before calling our validator
        # don't bother with our validation
        # if errors[key]:
        #     return

        value = data[key]
        if value is not missing:
            if isinstance(value, str):
                value = [value]
            elif not isinstance(value, list):
                errors[key].append(
                    _('Expecting list of strings, got "%s"') % str(value)
                )
                return
        else:
            value = []

        if not errors[key]:
            data[key] = json.dumps(value)

    return validator


def multilingual_text_output(value):
    """
    Return stored json representation as a multilingual dict, if
    value is already a dict just pass it through.
    """
    if isinstance(value, dict):
        return value
    return parse_json(value)


def timestamp_to_datetime(value):
    """
    Returns a datetime for a given timestamp
    """
    try:
        return datetime.datetime.fromtimestamp(int(value)).isoformat()
    except ValueError:
        return False


def temporals_to_datetime_output(value):
    """
    Converts a temporal with start and end date
    as timestamps to temporal as datetimes
    """
    value = parse_json(value)

    for temporal in value:
        for key in temporal:
            if temporal[key]:
                temporal[key] = timestamp_to_datetime(temporal[key])
            else:
                temporal[key] = None
    return value


@scheming_validator
def list_of_dicts(field, schema):
    def validator(key, data, errors, context):
        # if there was an error before calling our validator
        # don't bother with our validation
        if errors[key]:
            return

        try:
            data_dict = df.unflatten(data[("__junk",)])
            value = data_dict[key[0]]
            if value is not missing:
                if isinstance(value, str):
                    value = [value]
                elif not isinstance(value, list):
                    errors[key].append(
                        _('Expecting list of strings, got "%s"') % str(value)
                    )
                    return
            else:
                value = []

            if not errors[key]:
                data[key] = json.dumps(value)

            # remove from junk
            del data_dict[key[0]]
            data[("__junk",)] = df.flatten_dict(data_dict)
        except KeyError:
            pass

    return validator


def multiple_text_output(value):
    """
    Return stored json representation as a list
    """
    return parse_json(value)


@scheming_validator
def ogdch_multiple_choice(field, schema):
    """
    Accept zero or more values from a list of choices and convert
    to a json list for storage:
    1. a list of strings, eg.:
       ["choice-a", "choice-b"]
    2. a single string for single item selection in form submissions:
       "choice-a"
    """
    choice_values = set(c["value"] for c in field["choices"])

    def validator(key, data, errors, context):
        # if there was an error before calling our validator
        # don't bother with our validation
        if errors[key]:
            return

        value = data[key]
        if value is not missing and value is not None:
            if isinstance(value, str):
                value = [value]
            elif not isinstance(value, list):
                errors[key].append(
                    _('Expecting list of strings, got "%s"') % str(value)
                )
                return
        else:
            value = []

        selected = set()
        for element in value:
            if element in choice_values:
                selected.add(element)
                continue
            errors[key].append(_('unexpected choice "%s"') % element)

        if not errors[key]:
            data[key] = json.dumps(
                [c["value"] for c in field["choices"] if c["value"] in selected]
            )

    return validator


@scheming_validator
def ogdch_unique_identifier(field, schema):
    def validator(key, data, errors, context):
        id = data.get(key[:-1] + ("id",))
        identifier = data.get(key[:-1] + ("identifier",))
        try:
            result = get_action("ogdch_dataset_by_identifier")(
                {}, {"identifier": identifier}
            )
            if id != result["id"]:
                raise df.Invalid(_("Identifier is already in use, it must be unique."))
        except NotFound:
            pass

    return validator


@scheming_validator
def url_validator(field, schema):
    def validator(key, data, errors, context):
        if errors[key]:
            return
        value = data[key]
        if value and value is not missing:
            if not isinstance(value, str) or not re.match(r"^https?://.*$", value):
                raise df.Invalid(_("Invalid URL"))

    return validator


@scheming_validator
def ogdch_fluent_tags(field, schema):
    """
    To be called after ckanext-fluent fluent_tags() because of an error that
    does not save any tag data for a language that has no tags entered, e.g. it
    would save {"de": ["tag-de"]} if German were the only language with a tag
    entered in the form. Not saving tag data for all the languages causes the
    tags to later be interpreted as a string, so here the dataset would display
    the tag '{u"de": [u"tag-de"]}' in every language.

    What we need to do in this case is save the tag field thus:
    {"de": ["tag-de"], "fr": [], "en": [], "it": []}

    Tags are munged to contain only lowercase letters, numbers, and the
    characters `-_.`
    """

    def validator(key, data, errors, context):
        if errors[key]:
            return

        value = json.loads(data[key])
        new_value = {}
        for lang in schema["form_languages"]:
            new_value[lang] = []
            if lang not in value.keys():
                continue
            for keyword in value[lang]:
                new_value[lang].append(munge_tag(keyword))

        data[key] = json.dumps(new_value)

    return validator


def ogdch_name_validator(value, context):
    """Overridden version of ckan.logic.validators.name_validator that allows
    usernames to contain uppercase letters, spaces and the @ symbol.

    This is needed because we have a large number of user accounts that were originally
    created in WordPress (which has different requirements for usernames) and copied
    across to CKAN. Now we don't sync users between WP and CKAN, we need to be able to
    update those users in the CKAN GUI without running into validation errors.
    """
    # Check if we're validating a username for an existing user: we don't want to allow
    # uppercase letters for any other kind of name.
    existing_username = asbool(context.get("user_obj"))

    if not isinstance(value, str):
        raise df.Invalid(_("Names must be strings"))

    # check basic textual rules
    if value in ["new", "edit", "search"]:
        raise df.Invalid(_("That name cannot be used"))

    if len(value) < 2:
        raise df.Invalid(_("Must be at least %s characters long") % 2)
    if len(value) > PACKAGE_NAME_MAX_LENGTH:
        raise df.Invalid(
            _("Name must be a maximum of %i characters long") % PACKAGE_NAME_MAX_LENGTH
        )
    if existing_username:
        if not user_name_match.match(value):
            raise df.Invalid(
                _(
                    "Must be purely alphanumeric "
                    "(ascii) characters and these symbols: -_@"
                )
            )
    else:
        if not name_match.match(value):
            raise df.Invalid(
                _(
                    "Must be purely lowercase alphanumeric "
                    "(ascii) characters and these symbols: -_"
                )
            )

    return value
