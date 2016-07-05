from collections import defaultdict

from ckan.plugins.toolkit import missing, _
import ckan.lib.navl.dictization_functions as df
from ckanext.scheming.validation import scheming_validator
from ckanext.switzerland.helpers import parse_json
from ckan.logic import NotFound, get_action
import json
import datetime
import logging
log = logging.getLogger(__name__)


@scheming_validator
def swiss_date(field, schema):
    def validator(key, data, errors, context):
        value = data[key]
        if isinstance(value, datetime.date):
            return value
        try:
            return datetime.datetime.strptime(value, '%d.%m.%Y')
        except ValueError:
            errors[key].append(_('Invalid date'))
    return validator


@scheming_validator
def json_list_of_dicts_field(field, schema):

    field_type = {
        'temporals': {
            'start_date': lambda date: datetime.datetime.strptime(date, '%d.%m.%Y').isoformat(),
            'end_date': lambda date: datetime.datetime.strptime(date, '%d.%m.%Y').isoformat(),
        },
        'contact_points': {
            'email': lambda text: text,
            'name': lambda text: text,
        },
        'publishers': {
            'label': lambda text: text,
        },
        'relations': {
            'url': lambda text: text,
            'label': lambda text: text,
        },
    }[field['field_name']]

    def validator(key, data, errors, context):
        # code idea based on ckanext-fluents fluent_text validator

        value = data[key]

        # 1 or 2. dict or JSON encoded string
        if value is not missing:
            if isinstance(value, basestring):
                try:
                    value = json.loads(value)
                except ValueError:
                    errors[key].append(_('Failed to decode JSON string'))
                    return
                except UnicodeDecodeError:
                    errors[key].append(_('Invalid encoding for JSON string'))
                    return
            if not isinstance(value, dict):
                errors[key].append(_('expecting JSON object'))
                return

            # TODO: validate value

            if not errors[key]:
                data[key] = json.dumps(value)
            return

        """
        3. separate fields/parse form data
        we change the name attribute in the template, so the value is "missing"
        we get the actual values from the __extras dict

        """
        prefix = key[-1] + '-'
        extras = data.get(key[:-1] + ('__extras',), {})

        values = defaultdict(lambda: {})

        # iterate over all extra fields and find our fields
        for name, text in extras.iteritems():
            if not name.startswith(prefix):
                continue

            try:
                # field name example: temporals-1-start_date
                counter, json_field_name = name.split('-')[1:]
                counter = int(counter)
                if json_field_name not in field_type.keys():
                    raise ValueError
            except ValueError:
                errors[key].append(_('Invalid form data'))
                continue

            if not text:
                errors[key].append(_('This field is required'))
                continue

            try:
                # convert field value
                values[counter][json_field_name] = field_type[json_field_name](text)
            except ValueError:
                errors[key].append(_('Invalid date'))

        # iterate over junk fields (used in resource creation/update)
        junk = data.get(('__junk',))
        if junk:
            for (field_name, counter, json_field_name), value in junk.iteritems():
                if field_name == key[-1]:
                    values[counter][json_field_name] = value

        data[key] = json.dumps(values.values())
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
            if isinstance(value, basestring):
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
            data_dict = df.unflatten(data[('__junk',)])
            value = data_dict[key[0]]
            if value is not missing:
                if isinstance(value, basestring):
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
            data[('__junk',)] = df.flatten_dict(data_dict)
        except KeyError:
            pass

    return validator


def multiple_text_output(value):
    """
    Return stored json representation as a list
    """
    return parse_json(value, default_value=[value])


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
    choice_values = set(c['value'] for c in field['choices'])

    def validator(key, data, errors, context):
        # if there was an error before calling our validator
        # don't bother with our validation
        if errors[key]:
            return

        value = data[key]
        if value is not missing and value is not None:
            if isinstance(value, basestring):
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
            data[key] = json.dumps([
                c['value'] for c in field['choices'] if c['value'] in selected
            ])

    return validator


@scheming_validator
def ogdch_unique_identifier(field, schema):
    def validator(key, data, errors, context):
        id = data.get(key[:-1] + ('id',))
        identifier = data.get(key[:-1] + ('identifier',))
        try:
            result = get_action('ogdch_dataset_by_identifier')(
                {},
                {'identifier': identifier}
            )
            if id != result['id']:
                raise df.Invalid(
                    _('Identifier is already in use, it must be unique.')
                )
        except NotFound:
            pass

    return validator
