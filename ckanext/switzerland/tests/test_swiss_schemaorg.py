import rdflib

from ckanext.dcat.profiles import SCHEMA

from ckanext.switzerland.dcat.profiles import SwissSchemaOrgProfile


def _profile():
    return SwissSchemaOrgProfile(rdflib.Graph())


def test_basic_fields_map_fluent_description():
    """
    Make sure SwissSchemaOrgProfile maps multilang descriptions to schema:description.
    """
    profile = _profile()
    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")

    dataset_dict = {
        "name": "ds",
        "title": {"de": "Titel", "fr": "Titre"},
        "description": {"de": "DE text", "fr": "FR texte"},
        "resources": [],
    }

    profile._basic_fields_graph(dataset_ref, dataset_dict)

    translated_values = [
        (str(o.language), str(o))
        for o in profile.g.objects(dataset_ref, SCHEMA.description)
        if getattr(o, "language", None)
    ]
    assert ("de", "DE text") in translated_values
    assert ("fr", "FR texte") in translated_values


def test_basic_fields_map_fluent_title():
    profile = _profile()
    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")
    dataset_dict = {
        "name": "ds",
        "title": {"de": "Titel", "it": "Titolo"},
        "resources": [],
    }
    profile._basic_fields_graph(dataset_ref, dataset_dict)
    translated_values = [
        (str(o.language), str(o))
        for o in profile.g.objects(dataset_ref, SCHEMA.name)
        if getattr(o, "language", None)
    ]
    assert ("de", "Titel") in translated_values
    assert ("it", "Titolo") in translated_values


def test_empty_description_dict_emits_no_description_literals():
    """Multilingual serialization skips empty dicts (same rules as DCAT profile)."""
    profile = _profile()
    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")
    profile._basic_fields_graph(
        dataset_ref,
        {
            "name": "ds",
            "description": {},
            "resources": [],
        },
    )
    assert list(profile.g.objects(dataset_ref, SCHEMA.description)) == []


def test_description_empty_string_lang_skipped_whitespace_kept():
    """
    Empty value for ``fr`` is skipped. Whitespace-only ``de`` is truthy, so a
    language-tagged literal is emitted (``MultiLangProfile`` does not strip).
    """
    profile = _profile()
    dataset_ref = rdflib.URIRef("http://example.org/dataset/2")
    profile._basic_fields_graph(
        dataset_ref,
        {
            "name": "ds",
            "description": {"de": "   ", "en": "Kept", "fr": ""},
            "resources": [],
        },
    )
    translated_values = [
        (str(o.language), str(o))
        for o in profile.g.objects(dataset_ref, SCHEMA.description)
        if getattr(o, "language", None)
    ]
    assert set(translated_values) == {("de", "   "), ("en", "Kept")}


def test_distribution_basic_fields_multilang_name_and_description():
    profile = _profile()
    distribution = rdflib.URIRef("http://example.org/dist/1")
    resource_dict = {
        "title": {"de": "Ressource", "fr": "Ressource FR"},
        "description": {"de": "Beschreibung"},
    }
    profile._distribution_basic_fields_graph(distribution, resource_dict)

    name_translated_values = [
        (str(o.language), str(o))
        for o in profile.g.objects(distribution, SCHEMA.name)
        if getattr(o, "language", None)
    ]
    assert ("de", "Ressource") in name_translated_values
    assert ("fr", "Ressource FR") in name_translated_values

    desc_translated_values = [
        (str(o.language), str(o))
        for o in profile.g.objects(distribution, SCHEMA.description)
        if getattr(o, "language", None)
    ]
    assert desc_translated_values == [("de", "Beschreibung")]
