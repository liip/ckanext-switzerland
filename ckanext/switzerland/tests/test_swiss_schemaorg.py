import pytest
import rdflib

from ckanext.dcat.profiles import SCHEMA
from ckanext.dcat.utils import resource_uri
from ckanext.switzerland.dcat.profiles import SwissSchemaOrgProfile


def _tagged_lang_values(graph, subject, predicate):
    """helper for language-tagged literals on ``predicate``."""
    return [
        (str(o.language), str(o))
        for o in graph.objects(subject, predicate)
        if getattr(o, "language", None)
    ]


@pytest.fixture
def profile():
    return SwissSchemaOrgProfile(rdflib.Graph())


def test_graph_from_dataset_maps_multilang_description_and_title(profile):
    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")
    dataset_dict = {
        "name": "ds",
        "title": {"de": "Titel", "fr": "Titre"},
        "description": {"de": "DE text", "fr": "FR texte"},
        "resources": [],
    }

    profile.graph_from_dataset(dataset_dict, dataset_ref)

    assert ("de", "DE text") in _tagged_lang_values(
        profile.g, dataset_ref, SCHEMA.description
    )
    assert ("fr", "FR texte") in _tagged_lang_values(
        profile.g, dataset_ref, SCHEMA.description
    )
    assert ("de", "Titel") in _tagged_lang_values(profile.g, dataset_ref, SCHEMA.name)
    assert ("fr", "Titre") in _tagged_lang_values(profile.g, dataset_ref, SCHEMA.name)


def test_graph_from_dataset_empty_description_dict_emits_no_tagged_description(
    profile,
):
    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")
    dataset_dict = {
        "name": "ds",
        "description": {},
        "resources": [],
    }

    profile.graph_from_dataset(dataset_dict, dataset_ref)

    assert _tagged_lang_values(profile.g, dataset_ref, SCHEMA.description) == []


def test_graph_from_dataset_description_empty_lang_skipped_whitespace_kept(profile):
    """
    Empty value for ``fr`` is skipped. Whitespace-only ``de`` is truthy, so a
    language-tagged literal is emitted (``MultiLangProfile`` does not strip).
    """
    dataset_ref = rdflib.URIRef("http://example.org/dataset/2")
    dataset_dict = {
        "name": "ds",
        "description": {"de": "   ", "en": "Kept", "fr": ""},
        "resources": [],
    }

    profile.graph_from_dataset(dataset_dict, dataset_ref)

    translated_values = _tagged_lang_values(profile.g, dataset_ref, SCHEMA.description)
    assert set(translated_values) == {("de", "   "), ("en", "Kept")}


def test_graph_from_dataset_maps_multilang_resource_title_and_description(profile):
    resource_uri_str = "http://example.org/resource/1"
    resource_dict = {
        "id": "res-1",
        "uri": resource_uri_str,
        "url": "http://example.org/data.csv",
        "title": {"de": "Ressource", "fr": "Ressource FR"},
        "description": {"de": "Beschreibung"},
    }
    dataset_ref = rdflib.URIRef("http://example.org/dataset/3")
    dataset_dict = {
        "name": "ds",
        "title": {"de": "Dataset"},
        "resources": [resource_dict],
    }

    assert resource_uri(resource_dict) == resource_uri_str
    distribution = rdflib.URIRef(resource_uri_str)

    profile.graph_from_dataset(dataset_dict, dataset_ref)

    name_values = _tagged_lang_values(profile.g, distribution, SCHEMA.name)
    assert ("de", "Ressource") in name_values
    assert ("fr", "Ressource FR") in name_values

    desc_values = _tagged_lang_values(profile.g, distribution, SCHEMA.description)
    assert set(desc_values) == {("de", "Beschreibung")}
