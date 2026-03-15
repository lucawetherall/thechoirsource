"""Tests for pipeline/discover.py (using mocked YouTube API)."""

import os
import tempfile

import pytest

from pipeline.config import Config
from pipeline.discover import discover_videos, _parse_duration, _score_video, _parse_youtube_id
from pipeline.queue_manager import QueueManager


@pytest.fixture(autouse=True)
def mock_mode(monkeypatch):
    monkeypatch.setenv("MOCK_MODE", "true")


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def queue_manager(tmp_path):
    for name in ("pending.json", "approved.json", "archive.json"):
        (tmp_path / name).write_text("[]")
    return QueueManager(queue_dir=str(tmp_path))


# ------------------------------------------------------------------
# Unit: helpers
# ------------------------------------------------------------------

def test_parse_duration_full():
    assert _parse_duration("PT1H2M3S") == 3723


def test_parse_duration_minutes_only():
    assert _parse_duration("PT8M30S") == 510


def test_parse_duration_seconds_only():
    assert _parse_duration("PT45S") == 45


def test_parse_duration_empty():
    assert _parse_duration("") == 0


def test_parse_duration_none():
    assert _parse_duration(None) == 0


def test_parse_youtube_id_watch_url():
    assert _parse_youtube_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_parse_youtube_id_short_url():
    assert _parse_youtube_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_parse_youtube_id_shorts():
    assert _parse_youtube_id("https://www.youtube.com/shorts/abc123") == "abc123"


def test_parse_youtube_id_invalid():
    assert _parse_youtube_id("not-a-url") is None


def test_score_video_higher_views_scores_higher():
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc)
    s1 = _score_video(100000, now.isoformat(), now)
    s2 = _score_video(1000, now.isoformat(), now)
    assert s1 > s2


def test_score_video_recency_decay():
    from datetime import datetime, timezone, timedelta
    now = datetime.now(tz=timezone.utc)
    recent = now.isoformat()
    old = (now - timedelta(days=20)).isoformat()
    s_recent = _score_video(10000, recent, now)
    s_old = _score_video(10000, old, now)
    assert s_recent > s_old


# ------------------------------------------------------------------
# Integration: discover_videos in mock mode
# ------------------------------------------------------------------

def test_discover_returns_list(config, queue_manager):
    results = discover_videos(config, queue_manager)
    assert isinstance(results, list)
    assert len(results) > 0


def test_discover_returns_dicts_with_required_keys(config, queue_manager):
    results = discover_videos(config, queue_manager)
    required = {
        "youtube_id", "title", "channel_name", "channel_id",
        "description", "duration_seconds", "view_count",
        "published_at", "url", "source"
    }
    for item in results:
        assert required.issubset(item.keys()), f"Missing keys in: {item}"


def test_discover_deduplicates_existing_queue(config, tmp_path):
    for name in ("approved.json", "archive.json"):
        (tmp_path / name).write_text("[]")
    # Pre-populate pending with some mock IDs
    import json
    (tmp_path / "pending.json").write_text(
        json.dumps([{"youtube_id": "mock_vid_001"}, {"youtube_id": "mock_vid_002"}])
    )
    qm = QueueManager(queue_dir=str(tmp_path))
    results = discover_videos(config, qm)
    ids = [r["youtube_id"] for r in results]
    assert "mock_vid_001" not in ids
    assert "mock_vid_002" not in ids


def test_discover_max_results(config, queue_manager):
    from pipeline.discover import MAX_RESULTS
    results = discover_videos(config, queue_manager)
    assert len(results) <= MAX_RESULTS


def test_discover_no_duplicate_ids(config, queue_manager):
    results = discover_videos(config, queue_manager)
    ids = [r["youtube_id"] for r in results]
    assert len(ids) == len(set(ids))
