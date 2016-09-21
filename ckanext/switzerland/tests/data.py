from ckan.lib.munge import munge_name
from ckan.tests import factories

environment = 'Test'
folder = 'Dataset'
filename = 'Didok.csv'
dataset_name = 'Dataset'
dataset_data = 'Year;data\n2013;1\n2014;2\n2015;3\n'


def user():
    return factories.User()


def organization(user):
    return factories.Organization(
        users=[{'name': user['id'], 'capacity': 'admin'}],
        description={'de': '', 'it': '', 'fr': '', 'en': ''},
        title={'de': '', 'it': '', 'fr': '', 'en': ''}
    )


def dataset(slug=None):
    if not slug:
        slug = munge_name(dataset_name)
    return factories.Dataset(identifier=dataset_name,
                             name=slug,
                             title={'de': '', 'it': '', 'fr': '', 'en': ''},
                             description={'de': '', 'it': '', 'fr': '', 'en': ''},
                             contact_points=[{'name': 'Contact Name', 'email': 'contact@example.com'}],
                             publishers=[{'label': 'Publisher 1'}])


def resource(dataset):
    pass
