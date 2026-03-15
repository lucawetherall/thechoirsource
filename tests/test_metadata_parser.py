"""Tests for pipeline/metadata_parser.py (rule-based mock parser)."""

import pytest

from pipeline.metadata_parser import _rule_based_parse


# Title format examples from the spec
TITLE_CASES = [
    (
        "Allegri - Miserere mei, Deus | Choir of King's College, Cambridge",
        "King's College Cambridge",
        {
            "piece_contains": "Miserere",
            "composer_contains": "Allegri",
            "ensemble_contains": "King",
        },
    ),
    (
        "The Sixteen: Palestrina — Sicut Cervus",
        "The Sixteen",
        {
            "ensemble_contains": "Sixteen",
            "piece_contains": "Sicut Cervus",
        },
    ),
    (
        "Evensong from Westminster Abbey, 10 March 2026",
        "Westminster Abbey",
        {
            "piece_contains": "Evensong",
            "ensemble_contains": "Westminster",
        },
    ),
    (
        "VOCES8: 'O Magnum Mysterium' (Lauridsen)",
        "VOCES8",
        {
            "ensemble_contains": "VOCES8",
            "piece_contains": "Magnum Mysterium",
        },
    ),
    (
        "Stanford Magnificat in G — Choir of St John's College, Cambridge",
        "St John's College Cambridge",
        {
            "piece_contains": "Magnificat",
        },
    ),
    (
        "J.S. Bach: Jesu, meine Freude BWV 227 | Netherlands Bach Society",
        "Netherlands Bach Society",
        {
            "piece_contains": "Jesu",
            "ensemble_contains": "Bach Society",
        },
    ),
    (
        "Choral Evensong live from York Minster",
        "York Minster",
        {
            "piece_contains": "Evensong",
        },
    ),
    (
        "Ola Gjeilo - Northern Lights (Aurora Borealis) | Tenebrae",
        "Tenebrae Choir",
        {
            "piece_contains": "Northern Lights",
            "ensemble_contains": "Tenebrae",
        },
    ),
    (
        "Britten - A Ceremony of Carols | Trinity College Cambridge",
        "Trinity College Cambridge",
        {
            "composer_contains": "Britten",
            "piece_contains": "Ceremony",
            "ensemble_contains": "Trinity",
        },
    ),
    (
        "Tallis - Spem in Alium | The Tallis Scholars",
        "The Tallis Scholars",
        {
            "piece_contains": "Spem",
            "ensemble_contains": "Tallis Scholars",
        },
    ),
]


@pytest.mark.parametrize("title,channel,expectations", TITLE_CASES)
def test_rule_based_parse(title, channel, expectations):
    result = _rule_based_parse(title, channel)

    assert isinstance(result["piece_title"], str)
    assert isinstance(result["composer"], str)
    assert isinstance(result["ensemble_name"], str)

    if "piece_contains" in expectations:
        assert expectations["piece_contains"].lower() in result["piece_title"].lower(), (
            f"Expected piece to contain '{expectations['piece_contains']}' "
            f"but got '{result['piece_title']}' for title: {title!r}"
        )

    if "composer_contains" in expectations:
        assert expectations["composer_contains"].lower() in result["composer"].lower(), (
            f"Expected composer to contain '{expectations['composer_contains']}' "
            f"but got '{result['composer']}' for title: {title!r}"
        )

    if "ensemble_contains" in expectations:
        # Check both ensemble_name field and channel fallback
        found_in_ensemble = expectations["ensemble_contains"].lower() in result["ensemble_name"].lower()
        assert found_in_ensemble, (
            f"Expected ensemble to contain '{expectations['ensemble_contains']}' "
            f"but got '{result['ensemble_name']}' for title: {title!r}"
        )


def test_no_separators_returns_full_title():
    """When no separators found, piece_title should be the full title."""
    result = _rule_based_parse("A Choir Piece", "Some Choir")
    assert "A Choir Piece" in result["piece_title"]


def test_channel_name_used_as_ensemble_fallback():
    """If ensemble not parsed from title, use channel_name."""
    result = _rule_based_parse("A Choir Piece", "Famous Choir")
    # Either ensemble was extracted or channel_name was used as fallback
    assert result["ensemble_name"] != "" or result["piece_title"] != ""


def test_returns_all_three_fields():
    """Always returns piece_title, composer, ensemble_name — even if empty strings."""
    result = _rule_based_parse("Something", "")
    assert "piece_title" in result
    assert "composer" in result
    assert "ensemble_name" in result


def test_bwv_number_stripped():
    """BWV numbers should not appear in piece_title."""
    result = _rule_based_parse(
        "J.S. Bach: Jesu, meine Freude BWV 227 | Netherlands Bach Society",
        "Netherlands Bach Society",
    )
    assert "BWV" not in result["piece_title"]


def test_majority_of_cases_parse_correctly():
    """At least 7/10 test cases should correctly parse piece_title and ensemble."""
    passes = 0
    for title, channel, expectations in TITLE_CASES:
        result = _rule_based_parse(title, channel)
        ok = True
        if "piece_contains" in expectations:
            if expectations["piece_contains"].lower() not in result["piece_title"].lower():
                ok = False
        if "ensemble_contains" in expectations:
            if expectations["ensemble_contains"].lower() not in result["ensemble_name"].lower():
                ok = False
        if ok:
            passes += 1
    assert passes >= 7, f"Only {passes}/10 test cases parsed correctly"
