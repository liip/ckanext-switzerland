# coding=utf-8
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
dataset_content_4 = 'Year;Data\n2013;1\n2014;2;2015;3;2016;4\n'


bahnhof_file = """0000006   7.549783  47.216111 441    % St. Katharinen
0000007   9.733756  46.922368 744    % Fideris
0000011   7.389462  47.191804 467    % Grenchen Nord
0000016   6.513937  46.659019 499    % La Sarraz, Couronne
0000017   9.873959  48.577852 0      % Amstetten(W) Lokalbahn
0000022   7.589551  47.547405 277    % Basel SBB
0000023   7.463718  46.941186 555    % Bern, Brunnadernstrasse
0000024   7.395229  46.937482 562    % Bern Bümpliz Süd
0000025   7.476796  46.936765 556    % Bern, Egghölzli
0000026   7.449206  46.943904 534    % Bern, Helvetiaplatz
0000028   6.838953  46.949588 491    % Boudry Littorail
0000030   8.711041  46.766423 831    % Golzern, Talstation Seilbahn
0000033   9.680469  47.544341 0      % Lindau Hbf"""

bahnhof_file_csv = """Didok Number,Latitude,Longtitude,Height,Station
0000006,7.549783,47.216111,441,St. Katharinen
0000007,9.733756,46.922368,744,Fideris
0000011,7.389462,47.191804,467,Grenchen Nord
0000016,6.513937,46.659019,499,"La Sarraz, Couronne"
0000017,9.873959,48.577852,0,Amstetten(W) Lokalbahn
0000022,7.589551,47.547405,277,Basel SBB
0000023,7.463718,46.941186,555,"Bern, Brunnadernstrasse"
0000024,7.395229,46.937482,562,Bern Bümpliz Süd
0000025,7.476796,46.936765,556,"Bern, Egghölzli"
0000026,7.449206,46.943904,534,"Bern, Helvetiaplatz"
0000028,6.838953,46.949588,491,Boudry Littorail
0000030,8.711041,46.766423,831,"Golzern, Talstation Seilbahn"
0000033,9.680469,47.544341,0,Lindau Hbf
""".replace('\n', '\r\n')

infoplus_config = {
    '0': 'Didok Number',
    '10': 'Latitude',
    '20': 'Longtitude',
    '30': 'Height',
    '37': '<ignore>',
    '39': 'Station',
}


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
