from paste.registry import Registry
from ckan import model
from ckan.lib.munge import munge_name, munge_filename
from ckan.tests import factories

environment = 'Test'
folder = 'Dataset'
filename = 'Didok.csv'
dataset_name = 'Dataset'
dataset_content_1 = 'Year;data\n2013;1\n'
dataset_content_2 = 'Year;Data\n2013;1\n2014;2\n'
dataset_content_3 = 'Year;Data\n2013;1\n2014;2;2015;3\n'


def user():
    return factories.User()


def harvest_user():
    factories.User(id='harvest', sysadmin=True)

    # setup the pylons context object c, required by datapusher
    registry = Registry()
    registry.prepare()
    import pylons
    c = pylons.util.AttribSafeContextObj()
    registry.register(pylons.c, c)
    pylons.c.user = 'harvest'
    pylons.c.userobj = model.User.get('harvest')


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


def resource(dataset, filename='filenamethatshouldnotmatch.csv'):
    factories.Resource(package_id=dataset['id'],
                       identifier='AAAResource',
                       title={'de': 'AAAResource', 'en': 'AAAResource', 'fr': 'AAAResource',
                              'it': 'AAAResource'},
                       description={'de': 'AAAResource Desc', 'en': 'AAAResource Desc',
                                    'fr': 'AAAResource Desc', 'it': 'AAAResource Desc'},
                       state='active',
                       rights='Other (Open)',
                       license='Other (Open)',
                       coverage='Coverage',
                       url='http://ogdch.dev/dataset/testdataset/resource/download/{}'.format(munge_filename(filename)))
