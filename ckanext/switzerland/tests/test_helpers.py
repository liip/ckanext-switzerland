import pytest

from ckanext.switzerland.helpers import map_to_valid_format

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
