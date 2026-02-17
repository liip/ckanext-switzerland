import pytest
import rdflib

from ckanext.switzerland.dcat.profiles import (
    DCT,
    LANGUAGE_URI_MAPPING,
    SwissDCATAPProfile,
)


@pytest.fixture
def profile():
    """
    Fresh profile + empty graph for each test.
    """
    g = rdflib.Graph()
    p = SwissDCATAPProfile(g)
    return p


def test_dataset_languages_are_mapped_to_uris(profile):
    """
    Dataset-level languages in dataset_dict["language"] should be exported as
    DCT.language triples with URIRefs based on LANGUAGE_URI_MAPPING.
    """
    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")

    dataset_dict = {
        "name": "test-dataset",
        "organization": {"name": "org-id"},
        "language": ["de", "fr"],
        "resources": [],
    }

    profile.graph_from_dataset(dataset_dict, dataset_ref)

    langs_in_graph = list(profile.g.objects(dataset_ref, DCT.language))
    assert len(langs_in_graph) == len(dataset_dict["language"])

    expected_uris = {LANGUAGE_URI_MAPPING[code] for code in dataset_dict["language"]}
    actual_uris = {str(o) for o in langs_in_graph}

    assert actual_uris == expected_uris
    assert all(isinstance(o, rdflib.term.URIRef) for o in langs_in_graph)


def test_resource_uses_its_own_languages_over_dataset(profile, monkeypatch):
    """
    If a resource has its own 'language' list, those should be used
    (and mapped to URIs), not the dataset-level languages.
    """
    from ckanext.switzerland.dcat import profiles as profiles_module

    def fake_resource_uri(res):
        return res["uri"]

    monkeypatch.setattr(profiles_module, "resource_uri", fake_resource_uri)

    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")

    dataset_dict = {
        "name": "test-dataset",
        "organization": {"name": "org-id"},
        "language": ["de"],  # dataset language (should be ignored for this dist)
    }

    res_uri = "http://example.org/resource/1"
    resource_dict = {
        "uri": res_uri,
        # resource language should win
        "language": ["it"],
        "url_type": "api",
        "url": "http://example.org/api",
    }

    profile._add_distribution_to_graph(dataset_ref, resource_dict, dataset_dict)

    dist_ref = rdflib.URIRef(res_uri)
    langs_in_graph = list(profile.g.objects(dist_ref, DCT.language))

    assert len(langs_in_graph) == 1
    lang = langs_in_graph[0]
    assert isinstance(lang, rdflib.term.URIRef)
    assert str(lang) == LANGUAGE_URI_MAPPING["it"]


def test_resource_falls_back_to_dataset_languages(profile, monkeypatch):
    """
    If a resource has no 'language', fall back to dataset_dict['language'].
    """
    from ckanext.switzerland.dcat import profiles as profiles_module

    def fake_resource_uri(res):
        return res["uri"]

    monkeypatch.setattr(profiles_module, "resource_uri", fake_resource_uri)

    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")

    dataset_dict = {
        "name": "test-dataset",
        "organization": {"name": "org-id"},
        "language": ["fr"],  # fallback source
    }

    res_uri = "http://example.org/resource/2"
    resource_dict = {
        "uri": res_uri,
        # no 'language' key, so we should use dataset_dict["language"]
        "url_type": "api",
        "url": "http://example.org/api-2",
    }

    profile._add_distribution_to_graph(dataset_ref, resource_dict, dataset_dict)

    dist_ref = rdflib.URIRef(res_uri)
    langs_in_graph = list(profile.g.objects(dist_ref, DCT.language))

    assert len(langs_in_graph) == 1
    lang = langs_in_graph[0]
    assert isinstance(lang, rdflib.term.URIRef)
    assert str(lang) == LANGUAGE_URI_MAPPING["fr"]


def test_distribution_handles_unknown_language_codes_gracefully(profile, monkeypatch):
    """
    If a language code is not in LANGUAGE_URI_MAPPING, _get_language_uri returns None
    and no DCT.language triple should be emitted.
    """
    from ckanext.switzerland.dcat import profiles as profiles_module

    def fake_resource_uri(res):
        return res["uri"]

    monkeypatch.setattr(profiles_module, "resource_uri", fake_resource_uri)

    dataset_ref = rdflib.URIRef("http://example.org/dataset/1")

    dataset_dict = {
        "name": "test-dataset",
        "organization": {"name": "org-id"},
        # unknown code, not in LANGUAGE_URI_MAPPING
        "language": ["xx"],
    }

    res_uri = "http://example.org/resource/3"
    resource_dict = {
        "uri": res_uri,
        # no own language -> would fall back to dataset, but that is unmapped
        "url_type": "api",
        "url": "http://example.org/api-3",
    }

    profile._add_distribution_to_graph(dataset_ref, resource_dict, dataset_dict)

    dist_ref = rdflib.URIRef(res_uri)
    langs_in_graph = list(profile.g.objects(dist_ref, DCT.language))

    # no triples should be created for unknown language codes
    assert langs_in_graph == []
