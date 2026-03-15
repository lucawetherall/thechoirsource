"""Tests for pipeline/generate_copy.py (using mocked Claude API)."""

import os

import pytest

from pipeline.generate_copy import generate_post_copy, FIXED_HASHTAGS


@pytest.fixture(autouse=True)
def mock_mode(monkeypatch):
    monkeypatch.setenv("MOCK_MODE", "true")


def make_config():
    from pipeline.config import Config
    return Config()


METADATA_CASES = [
    {
        "piece_title": "Miserere mei, Deus",
        "composer": "Gregorio Allegri",
        "ensemble_name": "Choir of King's College, Cambridge",
        "title": "Allegri - Miserere mei, Deus | Choir of King's College, Cambridge",
        "description": "A performance from King's College Chapel.",
    },
    {
        "piece_title": "O Magnum Mysterium",
        "composer": "Morten Lauridsen",
        "ensemble_name": "VOCES8",
        "title": "VOCES8: 'O Magnum Mysterium' (Lauridsen)",
        "description": "VOCES8 perform Lauridsen's luminous O Magnum Mysterium.",
    },
    {
        "piece_title": "Evensong",
        "composer": "",
        "ensemble_name": "Westminster Abbey",
        "title": "Evensong from Westminster Abbey",
        "description": "Choral Evensong.",
    },
]


@pytest.mark.parametrize("metadata", METADATA_CASES)
def test_generate_copy_returns_required_fields(metadata):
    config = make_config()
    result = generate_post_copy(metadata, config)

    assert "caption" in result
    assert "hashtags" in result
    assert "hashtags_string" in result
    assert "full_post_text" in result


@pytest.mark.parametrize("metadata", METADATA_CASES)
def test_caption_is_non_empty(metadata):
    config = make_config()
    result = generate_post_copy(metadata, config)
    assert len(result["caption"]) > 0


@pytest.mark.parametrize("metadata", METADATA_CASES)
def test_hashtags_is_list(metadata):
    config = make_config()
    result = generate_post_copy(metadata, config)
    assert isinstance(result["hashtags"], list)
    assert len(result["hashtags"]) > 0


@pytest.mark.parametrize("metadata", METADATA_CASES)
def test_fixed_hashtags_included(metadata):
    config = make_config()
    result = generate_post_copy(metadata, config)
    hashtags_str = result["hashtags_string"]
    for tag in FIXED_HASHTAGS:
        assert tag in hashtags_str, f"Fixed hashtag {tag} not in output"


@pytest.mark.parametrize("metadata", METADATA_CASES)
def test_full_post_text_combines_caption_and_hashtags(metadata):
    config = make_config()
    result = generate_post_copy(metadata, config)
    assert result["caption"] in result["full_post_text"]
    assert result["hashtags_string"] in result["full_post_text"]


def test_ensemble_name_appears_in_caption():
    """Caption should credit the ensemble."""
    config = make_config()
    metadata = {
        "piece_title": "Sicut Cervus",
        "composer": "Palestrina",
        "ensemble_name": "The Sixteen",
        "title": "The Sixteen: Palestrina — Sicut Cervus",
        "description": "",
    }
    result = generate_post_copy(metadata, config)
    # The ensemble name should appear somewhere in the full post
    assert "Sixteen" in result["full_post_text"] or "Sixteen" in result["caption"]


def test_no_forbidden_words():
    """Should not use 'ethereal' or 'transcendent'."""
    config = make_config()
    metadata = METADATA_CASES[0]
    result = generate_post_copy(metadata, config)
    text = result["full_post_text"].lower()
    assert "ethereal" not in text
    assert "transcendent" not in text
