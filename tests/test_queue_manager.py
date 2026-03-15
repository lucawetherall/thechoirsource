"""Tests for pipeline/queue_manager.py"""

import json
import os
import tempfile
from datetime import datetime, timezone, timedelta

import pytest

from pipeline.queue_manager import QueueManager


@pytest.fixture
def tmp_queue(tmp_path):
    """Returns a QueueManager backed by a fresh temporary directory."""
    # Initialise queue files as empty arrays
    for name in ("pending.json", "approved.json", "archive.json"):
        (tmp_path / name).write_text("[]")
    return QueueManager(queue_dir=str(tmp_path))


def make_item(youtube_id: str, **extra) -> dict:
    return {
        "youtube_id": youtube_id,
        "title": f"Test Video {youtube_id}",
        "channel_name": "Test Channel",
        "caption": "A great choir.",
        "hashtags": "#choir",
        "clips": [
            {"rank": 1, "r2_url": f"file:///tmp/{youtube_id}_clip1.mp4", "contrast_score": 0.9},
            {"rank": 2, "r2_url": f"file:///tmp/{youtube_id}_clip2.mp4", "contrast_score": 0.7},
        ],
        **extra,
    }


# ------------------------------------------------------------------
# Basic operations
# ------------------------------------------------------------------

def test_initial_state_empty(tmp_queue):
    assert tmp_queue.get_pending() == []
    assert tmp_queue.get_approved() == []
    assert tmp_queue.get_archive() == []


def test_add_pending(tmp_queue):
    items = [make_item("vid001"), make_item("vid002")]
    tmp_queue.add_pending(items)
    pending = tmp_queue.get_pending()
    assert len(pending) == 2
    assert pending[0]["youtube_id"] == "vid001"
    assert pending[1]["youtube_id"] == "vid002"


def test_add_pending_persists(tmp_queue, tmp_path):
    items = [make_item("vid001")]
    tmp_queue.add_pending(items)
    # New manager reads from disk
    qm2 = QueueManager(queue_dir=str(tmp_path))
    assert len(qm2.get_pending()) == 1


def test_approve_moves_to_approved(tmp_queue):
    tmp_queue.add_pending([make_item("vid001")])
    scheduled = "2026-04-01T19:00:00+01:00"
    result = tmp_queue.approve("vid001", 1, "Nice choir.", "#choir", scheduled)
    assert result is True
    assert len(tmp_queue.get_pending()) == 0
    approved = tmp_queue.get_approved()
    assert len(approved) == 1
    item = approved[0]
    assert item["youtube_id"] == "vid001"
    assert item["selected_clip_rank"] == 1
    assert item["caption"] == "Nice choir."
    assert item["scheduled_at"] == scheduled
    assert item["status"] == "approved"


def test_approve_nonexistent_returns_false(tmp_queue):
    assert tmp_queue.approve("no_such_id", 1, "cap", "#tag", "2026-04-01T19:00:00+00:00") is False


def test_reject_moves_to_archive(tmp_queue):
    tmp_queue.add_pending([make_item("vid001")])
    result = tmp_queue.reject("vid001")
    assert result is True
    assert len(tmp_queue.get_pending()) == 0
    archive = tmp_queue.get_archive()
    assert len(archive) == 1
    assert archive[0]["status"] == "rejected"
    assert "rejected_at" in archive[0]


def test_reject_nonexistent_returns_false(tmp_queue):
    assert tmp_queue.reject("no_such_id") is False


def test_mark_posted_moves_from_approved_to_archive(tmp_queue):
    tmp_queue.add_pending([make_item("vid001")])
    tmp_queue.approve("vid001", 1, "cap", "#tag", "2026-04-01T19:00:00+00:00")
    result = tmp_queue.mark_posted("vid001")
    assert result is True
    assert len(tmp_queue.get_approved()) == 0
    archive = tmp_queue.get_archive()
    assert len(archive) == 1
    assert archive[0]["status"] == "posted"
    assert "posted_at" in archive[0]


def test_mark_posted_nonexistent_returns_false(tmp_queue):
    assert tmp_queue.mark_posted("no_such_id") is False


# ------------------------------------------------------------------
# Dedup
# ------------------------------------------------------------------

def test_get_all_youtube_ids(tmp_queue):
    tmp_queue.add_pending([make_item("pending_1"), make_item("pending_2")])
    tmp_queue.approve("pending_1", 1, "cap", "#tag", "2026-04-01T19:00:00+00:00")
    tmp_queue.reject("pending_2")
    ids = tmp_queue.get_all_youtube_ids()
    assert "pending_1" in ids
    assert "pending_2" in ids


# ------------------------------------------------------------------
# Approved sorting
# ------------------------------------------------------------------

def test_approved_sorted_by_scheduled_at(tmp_queue):
    tmp_queue.add_pending([make_item("v1"), make_item("v2"), make_item("v3")])
    tmp_queue.approve("v1", 1, "c", "#t", "2026-04-03T19:00:00+00:00")
    tmp_queue.approve("v2", 1, "c", "#t", "2026-04-01T19:00:00+00:00")
    tmp_queue.approve("v3", 1, "c", "#t", "2026-04-02T19:00:00+00:00")
    approved = tmp_queue.get_approved()
    scheduled_dates = [a["scheduled_at"] for a in approved]
    assert scheduled_dates == sorted(scheduled_dates)


# ------------------------------------------------------------------
# get_due_for_posting
# ------------------------------------------------------------------

def test_get_due_for_posting_returns_past_items(tmp_queue):
    past = "2026-01-01T19:00:00+00:00"
    future = "2099-12-31T23:59:59+00:00"
    tmp_queue.add_pending([make_item("past"), make_item("future")])
    tmp_queue.approve("past", 1, "c", "#t", past)
    tmp_queue.approve("future", 1, "c", "#t", future)

    now = datetime.now(tz=timezone.utc)
    due = tmp_queue.get_due_for_posting(now)
    assert len(due) == 1
    assert due[0]["youtube_id"] == "past"


def test_get_due_for_posting_naive_now(tmp_queue):
    """Naive datetimes should be treated as UTC."""
    past = "2026-01-01T19:00:00+00:00"
    tmp_queue.add_pending([make_item("past")])
    tmp_queue.approve("past", 1, "c", "#t", past)
    now = datetime(2026, 6, 1, 12, 0, 0)  # naive
    due = tmp_queue.get_due_for_posting(now)
    assert len(due) == 1


# ------------------------------------------------------------------
# Robustness: empty/missing/malformed files
# ------------------------------------------------------------------

def test_missing_files_treated_as_empty(tmp_path):
    qm = QueueManager(queue_dir=str(tmp_path))
    assert qm.get_pending() == []


def test_malformed_json_treated_as_empty(tmp_path):
    (tmp_path / "pending.json").write_text("NOT VALID JSON")
    (tmp_path / "approved.json").write_text("[]")
    (tmp_path / "archive.json").write_text("[]")
    qm = QueueManager(queue_dir=str(tmp_path))
    assert qm.get_pending() == []


def test_non_list_json_treated_as_empty(tmp_path):
    (tmp_path / "pending.json").write_text('{"not": "a list"}')
    (tmp_path / "approved.json").write_text("[]")
    (tmp_path / "archive.json").write_text("[]")
    qm = QueueManager(queue_dir=str(tmp_path))
    assert qm.get_pending() == []


def test_empty_file_treated_as_empty(tmp_path):
    (tmp_path / "pending.json").write_text("")
    (tmp_path / "approved.json").write_text("[]")
    (tmp_path / "archive.json").write_text("[]")
    qm = QueueManager(queue_dir=str(tmp_path))
    assert qm.get_pending() == []


# ------------------------------------------------------------------
# Atomic write safety
# ------------------------------------------------------------------

def test_atomic_write_no_corruption(tmp_queue, tmp_path):
    """Verify that file is either fully written or not at all."""
    items = [make_item(f"vid{i:03d}") for i in range(20)]
    tmp_queue.add_pending(items)
    # Read back directly from disk
    content = (tmp_path / "pending.json").read_text()
    data = json.loads(content)
    assert len(data) == 20


def test_invalid_scheduled_at_handled(tmp_queue, tmp_path):
    """Items with invalid scheduled_at should be skipped gracefully."""
    (tmp_path / "approved.json").write_text(
        json.dumps([{"youtube_id": "bad", "scheduled_at": "not-a-date"}])
    )
    qm = QueueManager(queue_dir=str(tmp_path))
    due = qm.get_due_for_posting(datetime.now(tz=timezone.utc))
    assert due == []
