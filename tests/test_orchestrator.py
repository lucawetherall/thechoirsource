"""
End-to-end tests for the pipeline orchestrator (pipeline/run.py).
All external API calls use mocks. Generates real video via MockDownloader + FFmpeg.
"""

import json
import os

import pytest

from pipeline.config import Config
from pipeline.queue_manager import QueueManager
from pipeline.run import run_weekly_pipeline, run_approval, run_rejection, run_publish


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("MOCK_MODE", "true")


@pytest.fixture
def config(tmp_path):
    """Config with queue pointing to a temp directory."""
    c = Config()
    # Override queue dir to use temp path
    c._queue_dir = tmp_path / "queue"
    c._queue_dir.mkdir()
    for name in ("pending.json", "approved.json", "archive.json"):
        (c._queue_dir / name).write_text("[]")
    return c


@pytest.fixture
def queue_manager(config):
    return QueueManager(queue_dir=str(config.queue_dir))


# ------------------------------------------------------------------
# End-to-end pipeline test
# ------------------------------------------------------------------

def test_weekly_pipeline_populates_pending(config, queue_manager):
    """Running the mock pipeline should populate pending.json."""
    run_weekly_pipeline(config)

    queue_manager.reload()
    pending = queue_manager.get_pending()

    assert len(pending) > 0, "Expected at least one item in pending queue"


def test_weekly_pipeline_items_have_required_fields(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()

    required_fields = {
        "youtube_id", "title", "channel_name", "caption",
        "hashtags", "clips", "piece_title", "ensemble_name", "added_at",
    }
    for item in pending:
        missing = required_fields - item.keys()
        assert not missing, f"Item {item.get('youtube_id')} missing fields: {missing}"


def test_weekly_pipeline_clips_have_r2_urls(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()

    for item in pending:
        clips = item.get("clips", [])
        assert len(clips) >= 1, f"No clips for {item['youtube_id']}"
        for clip in clips:
            assert "r2_url" in clip, f"No r2_url in clip: {clip}"
            assert clip["r2_url"], f"Empty r2_url in clip: {clip}"


def test_weekly_pipeline_clips_count(config, queue_manager):
    """Each item should have 1-3 clip candidates."""
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()

    for item in pending:
        clips = item.get("clips", [])
        assert 1 <= len(clips) <= 3, (
            f"Item {item['youtube_id']} has {len(clips)} clips — expected 1-3"
        )


def test_weekly_pipeline_items_have_caption(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()

    for item in pending:
        assert item.get("caption"), f"Empty caption for {item.get('youtube_id')}"


def test_weekly_pipeline_items_have_hashtags(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()

    for item in pending:
        assert item.get("hashtags"), f"Empty hashtags for {item.get('youtube_id')}"


def test_weekly_pipeline_items_have_parsed_metadata(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()

    # At least one item should have a non-empty piece_title or ensemble_name
    has_metadata = any(
        item.get("piece_title") or item.get("ensemble_name")
        for item in pending
    )
    assert has_metadata, "No items have parsed metadata"


def test_weekly_pipeline_no_duplicate_ids(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()
    ids = [item["youtube_id"] for item in pending]
    assert len(ids) == len(set(ids)), "Duplicate youtube_ids in pending queue"


# ------------------------------------------------------------------
# Approval workflow
# ------------------------------------------------------------------

def test_approve_moves_to_approved(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()
    assert pending, "No pending items to approve"

    youtube_id = pending[0]["youtube_id"]
    run_approval(config, youtube_id, 1, "Test caption.", "#choir")

    queue_manager.reload()
    assert not any(p["youtube_id"] == youtube_id for p in queue_manager.get_pending())
    approved = queue_manager.get_approved()
    assert any(a["youtube_id"] == youtube_id for a in approved)


def test_approved_item_has_scheduled_at(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()
    assert pending

    youtube_id = pending[0]["youtube_id"]
    run_approval(config, youtube_id, 1, "Test caption.", "#choir")

    queue_manager.reload()
    approved = queue_manager.get_approved()
    item = next(a for a in approved if a["youtube_id"] == youtube_id)
    assert "scheduled_at" in item
    assert item["scheduled_at"]
    # Should be ISO format with timezone
    from datetime import datetime
    dt = datetime.fromisoformat(item["scheduled_at"])
    assert dt.tzinfo is not None


# ------------------------------------------------------------------
# Rejection workflow
# ------------------------------------------------------------------

def test_reject_moves_to_archive(config, queue_manager):
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()
    assert pending

    youtube_id = pending[0]["youtube_id"]
    run_rejection(config, youtube_id)

    queue_manager.reload()
    assert not any(p["youtube_id"] == youtube_id for p in queue_manager.get_pending())
    archive = queue_manager.get_archive()
    assert any(a["youtube_id"] == youtube_id for a in archive)


# ------------------------------------------------------------------
# Publish workflow
# ------------------------------------------------------------------

def test_publish_due_items(config, queue_manager):
    """Publish should process due items and move them to archive."""
    run_weekly_pipeline(config)
    queue_manager.reload()
    pending = queue_manager.get_pending()
    assert pending

    # Approve with a past scheduled_at to make it due
    youtube_id = pending[0]["youtube_id"]
    # Manually add to approved with past date
    item = pending[0].copy()
    item["selected_clip_rank"] = 1
    item["scheduled_at"] = "2020-01-01T19:00:00+00:00"
    item["status"] = "approved"
    queue_manager._approved.append(item)
    queue_manager._pending.remove(pending[0])
    queue_manager._save()

    run_publish(config)

    queue_manager.reload()
    # Item should now be in archive as posted
    archive = queue_manager.get_archive()
    posted = next((a for a in archive if a["youtube_id"] == youtube_id), None)
    assert posted is not None
    assert posted["status"] == "posted"
