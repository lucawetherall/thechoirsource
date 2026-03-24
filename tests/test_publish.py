"""Tests for pipeline/publish.py (using mocked platform API responses)."""

import os
import pytest

from pipeline.publish import publish_to_instagram, publish_to_facebook, publish_to_tiktok, publish_clip


@pytest.fixture(autouse=True)
def mock_mode(monkeypatch):
    monkeypatch.setenv("MOCK_MODE", "true")


def make_config():
    from pipeline.config import Config
    return Config()


CLIP_URL = "file:///tmp/mock_r2/clips/test_vid/test_vid_clip1.mp4"
CAPTION = "A beautiful choir performance. Let this wash over you.\n\n#choir #classicalmusic"


def test_publish_instagram_mock_success():
    config = make_config()
    result = publish_to_instagram(CLIP_URL, CAPTION, config)
    assert result["platform"] == "instagram"
    assert result["success"] is True
    assert result["post_id"] is not None
    assert result["error"] is None


def test_publish_facebook_mock_success():
    config = make_config()
    result = publish_to_facebook(CLIP_URL, CAPTION, config)
    assert result["platform"] == "facebook"
    assert result["success"] is True
    assert result["post_id"] is not None
    assert result["error"] is None


def test_publish_tiktok_mock_success():
    config = make_config()
    result = publish_to_tiktok(CLIP_URL, CAPTION, config)
    assert result["platform"] == "tiktok"
    assert result["success"] is True
    assert result["post_id"] is not None
    assert result["error"] is None


def test_publish_clip_all_platforms():
    config = make_config()
    approved_item = {
        "youtube_id": "test_vid",
        "selected_clip_rank": 1,
        "caption": "Beautiful choir music.",
        "hashtags": "#choir #classicalmusic",
        "clips": [
            {"rank": 1, "r2_url": CLIP_URL, "contrast_score": 0.9},
            {"rank": 2, "r2_url": CLIP_URL.replace("clip1", "clip2"), "contrast_score": 0.7},
        ],
    }
    result = publish_clip(approved_item, config)
    assert result["youtube_id"] == "test_vid"
    assert isinstance(result["results"], list)
    assert len(result["results"]) == len(config.platforms)
    assert result["all_success"] is True


def test_publish_clip_no_matching_rank():
    """If selected_clip_rank doesn't match any clip, returns error."""
    config = make_config()
    approved_item = {
        "youtube_id": "no_clip",
        "selected_clip_rank": 99,
        "caption": "Test",
        "hashtags": "#test",
        "clips": [{"rank": 1, "r2_url": CLIP_URL}],
    }
    result = publish_clip(approved_item, config)
    assert result["all_success"] is False


def test_publish_clip_no_clips_list():
    """Missing clips list should fail gracefully."""
    config = make_config()
    approved_item = {
        "youtube_id": "no_clips",
        "selected_clip_rank": 1,
        "caption": "Test",
        "hashtags": "#test",
        "clips": [],
    }
    result = publish_clip(approved_item, config)
    assert result["all_success"] is False
