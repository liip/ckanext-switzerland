import pytest

from ckanext.switzerland.helpers import get_robots_meta_content, map_to_valid_format

CSV_URI = "http://publications.europa.eu/resource/authority/file-type/CSV"
GEOJSON_URI = "http://publications.europa.eu/resource/authority/file-type/GEOJSON"
GEOTIFF_URI = "http://publications.europa.eu/resource/authority/file-type/GEOTIFF"
GPKG_URI = "http://publications.europa.eu/resource/authority/file-type/GPKG"
HTML_URI = "http://publications.europa.eu/resource/authority/file-type/HTML"
JSON_URI = "http://publications.europa.eu/resource/authority/file-type/JSON"
KMZ_URI = "http://publications.europa.eu/resource/authority/file-type/KMZ"
ODS_URI = "http://publications.europa.eu/resource/authority/file-type/ODS"
PDF_URI = "http://publications.europa.eu/resource/authority/file-type/PDF"
PNG_URI = "http://publications.europa.eu/resource/authority/file-type/PNG"
RDF_URI = "http://publications.europa.eu/resource/authority/file-type/RDF"
TXT_URI = "http://publications.europa.eu/resource/authority/file-type/TXT"
TIFF_URI = "http://publications.europa.eu/resource/authority/file-type/TIFF"
WCS_URI = "http://publications.europa.eu/resource/authority/file-type/WCS_SRVC"
WFS_URI = "http://publications.europa.eu/resource/authority/file-type/WFS_SRVC"
WMS_URI = "http://publications.europa.eu/resource/authority/file-type/WMS_SRVC"
WMTS_URI = "http://publications.europa.eu/resource/authority/file-type/WMTS_SRVC"
XLS_URI = "http://publications.europa.eu/resource/authority/file-type/XLS"
XML_URI = "http://publications.europa.eu/resource/authority/file-type/XML"
ZIP_URI = "http://publications.europa.eu/resource/authority/file-type/ZIP"
INVALID_FORMAT = None


@pytest.mark.parametrize(
    "input_format,expected",
    [
        ("csv", CSV_URI),
        ("CSV", CSV_URI),
        ("geojson", GEOJSON_URI),
        ("geotiff", GEOTIFF_URI),
        ("gpkg", GPKG_URI),
        ("html", HTML_URI),
        ("json", JSON_URI),
        ("kmz", KMZ_URI),
        ("ods", ODS_URI),
        ("pdf", PDF_URI),
        ("png", PNG_URI),
        ("sparql-...", RDF_URI),
        ("text", TXT_URI),
        ("txt", TXT_URI),
        ("text (.txt)", TXT_URI),
        ("plain", TXT_URI),
        ("tiff", TIFF_URI),
        ("wcs", WCS_URI),
        ("wfs", WFS_URI),
        ("wms", WMS_URI),
        ("wmts", WMTS_URI),
        ("xls", XLS_URI),
        ("xlsx", XLS_URI),
        ("xml", XML_URI),
        ("zip", ZIP_URI),
        ("gz", ZIP_URI),
        (None, INVALID_FORMAT),
        ("", INVALID_FORMAT),
        ("exe", INVALID_FORMAT),
    ],
)
def test_map_to_valid_format_known_values(input_format, expected):
    assert map_to_valid_format(input_format) == expected


INDEX = "index, follow"
NOINDEX = "noindex, follow"


class _FakeArgs(dict):
    """Minimal stand-in for Flask request.args (truthy when non-empty)."""

    def __bool__(self):
        return bool(dict(self))


class _FakeRequest:
    def __init__(self, endpoint, path="/", args=None):
        self.endpoint = endpoint
        self.path = path
        self.args = _FakeArgs(args or {})


@pytest.mark.parametrize(
    "endpoint,path,args,expected",
    [
        ("dataset.search", "/dataset/", None, INDEX),
        ("dataset.search", "/dataset/", {"q": "fahrplan"}, NOINDEX),
        ("dataset.search", "/dataset/", {"groups": "timetables"}, NOINDEX),
        ("dataset.search", "/dataset/", {"page": "2"}, NOINDEX),
        ("ogdch_home.search", "/", None, INDEX),
        ("ogdch_home.search", "/", {"organization": "oevch"}, NOINDEX),
        ("dataset.read", "/dataset/bike-and-car-parking", None, INDEX),
        ("showcase_blueprint.index", "/showcase/", None, INDEX),
        (
            "showcase_blueprint.index",
            "/showcase/",
            {"q": "viz"},
            NOINDEX,
        ),
        (
            "showcase_blueprint.read",
            "/showcase/visualisierung-der-oev-tagesentwicklungen",
            None,
            INDEX,
        ),
        ("group.index", "/group/", None, INDEX),
        ("group.read", "/group/accessibilitydata", None, INDEX),
        (
            "group.read",
            "/group/accessibilitydata",
            {"q": "test"},
            NOINDEX,
        ),
        ("organization.index", "/organization/", None, NOINDEX),
        ("organization.read", "/organization/oevch", None, NOINDEX),
        (
            "organization.read",
            "/organization/oevch",
            {"q": "x"},
            NOINDEX,
        ),
        ("harvest.search", "/harvest/", None, NOINDEX),
        ("harvest.read", "/harvest/some-source", None, NOINDEX),
        # Path fallback when endpoint is missing/unknown
        (None, "/harvest/", None, NOINDEX),
        (None, "/organization/oevch", None, NOINDEX),
        (
            "dataset_resource.read",
            "/dataset/bike-and-car-parking/resource/eb892409-d24f-4484-a19b-2d6e26108d9a",
            None,
            NOINDEX,
        ),
        (
            "resource.read",
            "/dataset/bike-and-car-parking/resource/eb892409-d24f-4484-a19b-2d6e26108d9a",
            None,
            NOINDEX,
        ),
        # Path fallback when endpoint is missing/unknown but URL is a resource
        (
            None,
            "/dataset/bike-and-car-parking/resource/eb892409-d24f-4484-a19b-2d6e26108d9a",
            None,
            NOINDEX,
        ),
        ("user.read", "/user/admin", None, NOINDEX),
    ],
)
def test_get_robots_meta_content(monkeypatch, endpoint, path, args, expected):
    monkeypatch.setattr(
        "ckanext.switzerland.helpers.tk.request",
        _FakeRequest(endpoint, path, args),
    )
    assert get_robots_meta_content() == expected
