# coding=utf-8
from ckan import model
from ckan.lib.munge import munge_filename, munge_name
from ckan.tests import factories
from paste.registry import Registry

environment = "Test"
folder = "Dataset"
filename = "Didok.csv"
dataset_name = "Dataset"
dataset_content_1 = "Year;data\n2013;1\n"
dataset_content_2 = "Year;Data\n2013;1\n2014;2\n"
dataset_content_3 = "Year;Data\n2013;1\n2014;2;2015;3\n"
dataset_content_4 = "Year;Data\n2013;1\n2014;2;2015;3;2016;4\n"


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
0000033   9.680469  47.544341 0      % Lindau Hbf""".encode(
    "iso-8859-1"
)

bahnhof_file_csv = """StationID,Longitude,Latitude,Höhe,Remark
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
""".replace(
    "\n", "\r\n"
).encode(
    "utf-8"
)

infoplus_config = [
    {"from": 1, "to": 7, "name": "StationID"},
    {"from": 8, "to": 18, "name": "Longitude"},
    {"from": 20, "to": 29, "name": "Latitude"},
    {"from": 31, "to": 36, "name": "Höhe"},
    {"from": 40, "to": -1, "name": "Remark"},
]

ist_file = """BETRIEBSTAG;FAHRT_BEZEICHNER;BETREIBER_ID;BETREIBER_ABK;BETREIBER_NAME;PRODUKT_ID;LINIEN_ID;LINIEN_TEXT;UMLAUF_ID;VERKEHRSMITTEL_TEXT;ZUSATZFAHRT_TF;FAELLT_AUS_TF;BPUIC;HALTESTELLEN_NAME;ANKUNFTSZEIT;AN_PROGNOSE;AN_PROGNOSE_STATUS;ABFAHRTSZEIT;AB_PROGNOSE;AB_PROGNOSE_STATUS;DURCHFAHRT_TF
25.09.2016;80:06____:17010:000;80:06____;DB;DB Regio AG;Zug;17010;RE;;RE;false;false;8500090;Basel Bad Bf;;;PROGNOSE;25.09.2016 05:49;;UNBEKANNT;false
25.09.2016;80:06____:17010:000;80:06____;DB;DB Regio AG;Zug;17010;RE;;RE;false;false;8014428;Weil am Rhein;25.09.2016 05:52;;UNBEKANNT;25.09.2016 05:53;;UNBEKANNT;false
25.09.2016;80:06____:17010:000;80:06____;DB;DB Regio AG;Zug;17010;RE;;RE;false;false;8014424;Haltingen;25.09.2016 05:55;;UNBEKANNT;25.09.2016 05:55;;UNBEKANNT;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;857279002;Schlüsselacher;25.09.2016 22:15;25.09.2016 22:17:04;PROGNOSE;25.09.2016 22:15;25.09.2016 22:17:04;PROGNOSE;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;859179002;Friedhof;25.09.2016 22:16;25.09.2016 22:17:32;PROGNOSE;25.09.2016 22:16;25.09.2016 22:17:32;PROGNOSE;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;859178902;Leuenplatz;25.09.2016 22:17;25.09.2016 22:18:32;PROGNOSE;25.09.2016 22:17;25.09.2016 22:18:32;PROGNOSE;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;857278802;Bahnhof;25.09.2016 22:19;25.09.2016 22:19:55;PROGNOSE;25.09.2016 22:20;25.09.2016 22:20:25;PROGNOSE;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503020;Zürich Hardbrücke;25.09.2016 17:39;25.09.2016 17:40:35;GESCHAETZT;25.09.2016 17:39;25.09.2016 17:41:24;GESCHAETZT;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503000;Zürich HB;25.09.2016 17:43;25.09.2016 17:43:13;GESCHAETZT;25.09.2016 17:45;25.09.2016 17:45:39;GESCHAETZT;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503003;Zürich Stadelhofen;25.09.2016 17:47;25.09.2016 17:48:09;GESCHAETZT;25.09.2016 17:48;25.09.2016 17:49:54;GESCHAETZT;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503004;Zürich Tiefenbrunnen;25.09.2016 17:50;25.09.2016 17:52:03;GESCHAETZT;25.09.2016 17:51;25.09.2016 17:52:59;GESCHAETZT;false"""

ist_file_output = """BETRIEBSTAG;FAHRT_BEZEICHNER;BETREIBER_ID;BETREIBER_ABK;BETREIBER_NAME;PRODUKT_ID;LINIEN_ID;LINIEN_TEXT;UMLAUF_ID;VERKEHRSMITTEL_TEXT;ZUSATZFAHRT_TF;FAELLT_AUS_TF;BPUIC;HALTESTELLEN_NAME;ANKUNFTSZEIT;AN_PROGNOSE;AN_PROGNOSE_STATUS;ABFAHRTSZEIT;AB_PROGNOSE;AB_PROGNOSE_STATUS;DURCHFAHRT_TF
25.09.2016;80:06____:17010:000;80:06____;DB;DB Regio AG;Zug;17010;RE;;RE;false;false;8500090;Basel Bad Bf;;;PROGNOSE;25.09.2016 05:49;;UNBEKANNT;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;8572790;Schlüsselacher;25.09.2016 22:15;25.09.2016 22:17:04;PROGNOSE;25.09.2016 22:15;25.09.2016 22:17:04;PROGNOSE;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;8591790;Friedhof;25.09.2016 22:16;25.09.2016 22:17:32;PROGNOSE;25.09.2016 22:16;25.09.2016 22:17:32;PROGNOSE;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;8591789;Leuenplatz;25.09.2016 22:17;25.09.2016 22:18:32;PROGNOSE;25.09.2016 22:17;25.09.2016 22:18:32;PROGNOSE;false
25.09.2016;85:819:429889-07013-1;85:819;ARAG;Automobil Rottal AG;BUS;85:819:63;63;;;false;false;8572788;Bahnhof;25.09.2016 22:19;25.09.2016 22:19:55;PROGNOSE;25.09.2016 22:20;25.09.2016 22:20:25;PROGNOSE;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503020;Zürich Hardbrücke;25.09.2016 17:39;25.09.2016 17:40:35;GESCHAETZT;25.09.2016 17:39;25.09.2016 17:41:24;GESCHAETZT;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503000;Zürich HB;25.09.2016 17:43;25.09.2016 17:43:13;GESCHAETZT;25.09.2016 17:45;25.09.2016 17:45:39;GESCHAETZT;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503003;Zürich Stadelhofen;25.09.2016 17:47;25.09.2016 17:48:09;GESCHAETZT;25.09.2016 17:48;25.09.2016 17:49:54;GESCHAETZT;false
25.09.2016;85:11:19667:001;85:11;SBB;Schweizerische Bundesbahnen SBB;Zug;19667;S16;;S;false;false;8503004;Zürich Tiefenbrunnen;25.09.2016 17:50;25.09.2016 17:52:03;GESCHAETZT;25.09.2016 17:51;25.09.2016 17:52:59;GESCHAETZT;false
""".replace(
    "\n", "\r\n"
)


def user():
    return factories.User()


def harvest_user():
    factories.User(id="harvest", sysadmin=True)

    # setup the pylons context object c, required by datapusher
    registry = Registry()
    registry.prepare()
    import pylons

    c = pylons.util.AttribSafeContextObj()
    registry.register(pylons.c, c)
    pylons.c.user = "harvest"
    pylons.c.userobj = model.User.get("harvest")


def organization(user):
    return factories.Organization(
        users=[{"name": user["id"], "capacity": "admin"}],
        description={"de": "", "it": "", "fr": "", "en": ""},
        title={"de": "", "it": "", "fr": "", "en": ""},
    )


def dataset(slug=None):
    if not slug:
        slug = munge_name(dataset_name)
    return factories.Dataset(
        identifier=dataset_name,
        name=slug,
        title={"de": "", "it": "", "fr": "", "en": ""},
        description={"de": "", "it": "", "fr": "", "en": ""},
        contact_points=[{"name": "Contact Name", "email": "contact@example.com"}],
        publishers=[{"label": "Publisher 1"}],
    )


def resource(dataset, filename="filenamethatshouldnotmatch.csv"):
    factories.Resource(
        package_id=dataset["id"],
        identifier="AAAResource",
        title={
            "de": "AAAResource",
            "en": "AAAResource",
            "fr": "AAAResource",
            "it": "AAAResource",
        },
        description={
            "de": "AAAResource Desc",
            "en": "AAAResource Desc",
            "fr": "AAAResource Desc",
            "it": "AAAResource Desc",
        },
        state="active",
        rights="Other (Open)",
        license="Other (Open)",
        coverage="Coverage",
        url="http://odp.test/dataset/testdataset/resource/download/{}".format(
            munge_filename(filename)
        ),
    )
